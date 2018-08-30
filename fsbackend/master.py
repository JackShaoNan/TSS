"""Treadmill master process.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import json
import logging
import os
import re
import time
import zlib

import six

from treadmill import appevents
from treadmill import scheduler
from treadmill import utils
from treadmill import zknamespace as z
from treadmill import zkutils

from treadmill.appcfg import abort as app_abort
from treadmill.apptrace import events as traceevents

from . import loader


_LOGGER = logging.getLogger(__name__)

count = 0

def _time_past(when):
    """Check if time past the given timestamp."""
    return time.time() > when


# Timer interval to reevaluate time events (seconds).
# TIMER_INTERVAL = 60

# Time interval between running the scheduler (seconds).
_SCHEDULER_INTERVAL = 2

# Save reports on the scheduler state to ZooKeeper every minute.
_STATE_REPORT_INTERVAL = 60

# Interval to sleep before checking if there is new event in the queue.
_CHECK_EVENT_INTERVAL = 0.5

# Check integrity of the scheduler every 5 minutes.
_INTEGRITY_INTERVAL = 5 * 60

# Check for reboots every hour.
_REBOOT_CHECK_INTERVAL = 60 * 60

# Max number of events to process before checking if scheduler is due.
_EVENT_BATCH_COUNT = 20


class Master(loader.Loader):
    """Treadmill master scheduler."""

    def __init__(self, backend, cellname, events_dir=None):

        super(Master, self).__init__(backend, cellname)

        self.backend = backend
        self.events_dir = events_dir

        self.queue = collections.deque()
        self.up_to_date = False
        self.exit = False
        # Signals that processing of a given event.
        self.process_complete = dict()

        self.event_handlers = {
            z.SERVER_PRESENCE: self.process_server_presence,
            z.SCHEDULED: self.process_scheduled,
            z.EVENTS: self.process_events,
        }

    def create_rootns(self):
        """Create root nodes and set appropriate acls."""
        root_ns = [
            '/',
            z.ALLOCATIONS,
            z.APPMONITORS,
            z.BUCKETS,
            z.CELL,
            z.DISCOVERY,
            z.DISCOVERY_STATE,
            z.IDENTITY_GROUPS,
            z.PLACEMENT,
            z.PARTITIONS,
            z.SCHEDULED,
            z.SCHEDULER,
            z.SERVERS,
            z.STATE_REPORTS,
            z.STRATEGIES,
            z.FINISHED,
            z.FINISHED_HISTORY,
            z.TRACE,
            z.TRACE_HISTORY,
            z.TRAITS,
            z.VERSION_ID,
            z.ZOOKEEPER,
            z.BLACKEDOUT_SERVERS,
            z.ENDPOINTS,
            z.path.endpoint_proid('root'),
            z.EVENTS,
            z.RUNNING,
            z.SERVER_PRESENCE,
            z.VERSION,
            z.VERSION_HISTORY,
            z.REBOOTS,
        ]

        for path in root_ns:
            self.backend.ensure_exists(path)

        for path in z.trace_shards():
            self.backend.ensure_exists(path)

    @utils.exit_on_unhandled
    def process(self, event):
        """Process state change event."""
        path, children = event
        _LOGGER.info('processing: %r', event)

        assert path in self.event_handlers
        self.event_handlers[path](children)

        _LOGGER.info('waiting for completion.')
        self.process_complete[path].set()
        self.up_to_date = False

        _LOGGER.info('done processing events.')

    def process_scheduled(self, scheduled):
        """Callback invoked when on scheduling changes."""
        current = set(self.cell.apps.keys())
        target = set(scheduled)

        for appname in current - target:
            self.remove_app(appname)

        for appname in target - current:
            self.load_app(appname)

        # Store by-proid aggregates.
        aggregate = self._calculate_aggregate(target)
        self.backend.put(z.SCHEDULED_STATS, aggregate)

    def process_server_presence(self, servers):
        """Callback invoked when server presence is modified."""
        self.adjust_presence(set(servers))

    def process_events(self, events):
        """Callback invoked on state change/admin event."""
        # Events are sequential nodes in the form <prio>-<event>-<seq #>
        #
        # They are processed in order of (prio, seq_num, event)
        ordered = sorted([tuple([event.split('-')[i] for i in [0, 2, 1]])
                          for event in events
                          if re.match(r'\d+\-\w+\-\d+$', event)])

        for prio, seq, resource in ordered:
            _LOGGER.info('event: %s %s %s', prio, seq, resource)
            node_name = '-'.join([prio, resource, seq])
            if resource == 'allocations':
                # Changing allocations has potential of complete
                # reshuffle, so while ineffecient, reload all apps as well.
                #
                # If application is assigned to different partition, from
                # scheduler perspective is no different than host deleted. It
                # will be detected on schedule and app will be assigned new
                # host from proper partition.
                self.load_allocations()
                self.load_apps()
            elif resource == 'apps':
                # The event node contains list of apps to be re-evaluated.
                apps = self.backend.get_default(
                    z.path.event(node_name),
                    default=[])
                for app in apps:
                    self.load_app(app)
            elif resource == 'cell':
                self.load_cell()
            elif resource == 'buckets':
                self.load_buckets()
            elif resource == 'servers':
                servers = self.backend.get_default(
                    z.path.event(node_name),
                    default=[])
                if not servers:
                    # If not specified, reload all. Use union of servers in
                    # the model and in zookeeper.
                    servers = (set(self.servers.keys()) ^
                               set(self.backend.list(z.SERVERS)))
                self.reload_servers(servers)
            elif resource == 'identity_groups':
                self.load_identity_groups()
            else:
                _LOGGER.warning('Unsupported event resource: %s', resource)

        for node in events:
            _LOGGER.info('Deleting event: %s', z.path.event(node))
            self.backend.delete(z.path.event(node))

    def watch(self, path):
        """Constructs a watch on a given path."""


        @self.backend.ChildrenWatch(path)
        @utils.exit_on_unhandled
        def _watch(children):
            """Watch children events."""
            global count
            _LOGGER.debug('watcher begin: %s, %d', path, count)
            count += 1
            # On first invocation, we create event and do not wait on it,
            # as the event loop not started yet.
            #
            # On subsequent calls, wait for processing to complete before
            # renewing the watch, to avoid busy loops.
            if path in self.process_complete:
                self.process_complete[path].clear()

            self.queue.append((path, children))

            if path in self.process_complete:
                _LOGGER.debug('watcher waiting for completion: %s', path)
                self.process_complete[path].wait()
            else:
                self.process_complete[path] = \
                    self.backend.event_object()

            _LOGGER.debug('watcher finished: %s', path)
            return True

    def attach_watchers(self):
        """Attach watchers that push ZK children events into a queue."""
        self.watch(z.SERVER_PRESENCE)
        self.watch(z.SCHEDULED)
        self.watch(z.EVENTS)

    def store_timezone(self):
        """Store local timezone in root ZK node."""
        tz = time.tzname[0]
        self.backend.update('/', {'timezone': tz})

    @utils.exit_on_unhandled
    def run_loop(self, once=False):
        """Run the master loop."""
        self.create_rootns()
        self.store_timezone()
        self.load_model()
        self.init_schedule()
        self.attach_watchers()

        last_sched_time = time.time()
        last_integrity_check = 0
        last_reboot_check = 0
        last_state_report = 0
        if once:
            return

        while not self.exit:
            # Process ZK children events queue
            queue_empty = False
            for _idx in range(0, _EVENT_BATCH_COUNT):
                try:
                    event = self.queue.popleft()
                    self.process(event)
                except IndexError:
                    _LOGGER.info('nothing to pop')
                    queue_empty = True
                    break

            # Run periodic tasks

            if _time_past(last_sched_time + _SCHEDULER_INTERVAL):
                last_sched_time = time.time()
                if not self.up_to_date:
                    self.reschedule()
                    self.check_placement_integrity()

            if _time_past(last_state_report + _STATE_REPORT_INTERVAL):
                last_state_report = time.time()
                self.save_state_reports()

            if _time_past(last_integrity_check + _INTEGRITY_INTERVAL):
                assert self.check_integrity()
                self.tick_reboots()
                last_integrity_check = time.time()

            if _time_past(last_reboot_check + _REBOOT_CHECK_INTERVAL):
                self.check_reboot()
                last_reboot_check = time.time()

            if queue_empty:
                time.sleep(_CHECK_EVENT_INTERVAL)

    @utils.exit_on_unhandled
    def run(self, once=False, has_lock=True):
        """Runs the master (once it is elected leader)."""
        if has_lock: 
            lock = zkutils.make_lock(self.backend.zkclient,
                                     z.path.election(__name__))
            _LOGGER.info('Waiting for leader lock.')
            with lock:
                self.run_loop(once)
        else:
            self.run_loop(once)

    def tick_reboots(self):
        """Tick partition reboot schedulers."""
        now = time.time()
        for partition in self.cell.partitions.values():
            partition.tick(now)

    def _schedule_reboot(self, servername):
        """Schedule server reboot."""
        self.backend.ensure_exists(z.path.reboot(servername))

    def check_reboot(self):
        """Identify all expired servers."""
        self.cell.resolve_reboot_conflicts()

        now = time.time()

        # expired servers rebooted unconditionally, as they are no use anumore.
        for name, server in six.iteritems(self.servers):
            # Ignore servers that are not yet assigned to the reboot buckets.
            if server.valid_until == 0:
                _LOGGER.info(
                    'Server reboot bucket not initialized: %s', name
                )
                continue

            if now > server.valid_until:
                _LOGGER.info(
                    'Expired: %s at %s',
                    name,
                    server.valid_until
                )
                self._schedule_reboot(name)
                continue

    def _placement_data(self, app):
        """Return placement data for given app."""
        return {
            'identity': self.cell.apps[app].identity,
            'expires': self.cell.apps[app].placement_expiry
        }

    def _save_placement(self, placement):
        """Store latest placement as reference."""
        placement_data = json.dumps(placement)
        placement_zdata = zlib.compress(placement_data.encode())
        self.backend.put(z.path.placement(), placement_zdata)

    def init_schedule(self):
        """Run scheduler first time and update scheduled data."""
        placement = self.cell.schedule()

        for servername, server in six.iteritems(self.cell.members()):
            placement_node = z.path.placement(servername)
            self.backend.ensure_exists(placement_node)

            current = set(self.backend.list(placement_node))
            correct = set(server.apps.keys())

            for app in current - correct:
                _LOGGER.info('Unscheduling: %s - %s', servername, app)
                self.backend.delete(os.path.join(placement_node, app))
            for app in correct - current:
                _LOGGER.info('Scheduling: %s - %s,%s',
                             servername, app, self.cell.apps[app].identity)

                placement_data = self._placement_data(app)
                self.backend.put(
                    os.path.join(placement_node, app),
                    placement_data
                )

                self._update_task(app, servername, why=None)

        self._save_placement(placement)
        self.up_to_date = True

    def reschedule(self):
        """Run scheduler and adjust placement."""
        placement = self.cell.schedule()

        # Filter out placement records where nothing changed.
        changed_placement = [
            (app, before, exp_before, after, exp_after)
            for app, before, exp_before, after, exp_after in placement
            if before != after or exp_before != exp_after
        ]

        # We run two loops. First - remove all old placement, before creating
        # any new ones. This ensures that in the event of loop interruption
        # for anyreason (like Zookeeper connection lost or master restart)
        # there are no duplicate placements.
        for app, before, _exp_before, after, _exp_after in changed_placement:
            if before and before != after:
                _LOGGER.info('Unscheduling: %s - %s', before, app)
                self.backend.delete(z.path.placement(before, app))

        for app, before, _exp_before, after, exp_after in changed_placement:
            placement_data = self._placement_data(app)

            why = ''
            if before is not None:
                if (before not in self.servers or
                        self.servers[before].state == scheduler.State.down):
                    why = '{server}:down'.format(server=before)
                else:
                    # TODO: it will be nice to put app utilization at the time
                    #       of eviction, but this info is not readily
                    #       available yet in the scheduler.
                    why = 'evicted'

            if after:
                _LOGGER.info('Scheduling: %s - %s,%s, expires at: %s',
                             after,
                             app,
                             self.cell.apps[app].identity,
                             exp_after)

                self.backend.put(
                    z.path.placement(after, app),
                    placement_data
                )
                self._update_task(app, after, why=why)
            else:
                self._update_task(app, None, why=why)

        self._unschedule_evicted()

        self._save_placement(placement)
        self.up_to_date = True

    def _unschedule_evicted(self):
        """Delete schedule once and evicted apps."""
        # Apps that were evicted and are configured to be scheduled once
        # should be removed.
        #
        # Remove will trigger rescheduling which will be harmless but
        # strictly speaking unnecessary.
        for appname, app in six.iteritems(self.cell.apps):
            if app.schedule_once and app.evicted:
                _LOGGER.info('Removing schedule_once/evicted app: %s',
                             appname)
                # TODO: unfortunately app.server is already None at this point.
                self.backend.put(
                    z.path.finished(appname),
                    {'state': 'terminated',
                     'when': time.time(),
                     'host': None,
                     'data': 'schedule_once'},
                )
                self.backend.delete(z.path.scheduled(appname))

    def _update_task(self, appname, server, why):
        """Creates/updates application task with the new placement."""
        # Servers in the cell have full control over task node.
        if self.events_dir:
            if server:
                appevents.post(
                    self.events_dir,
                    traceevents.ScheduledTraceEvent(
                        instanceid=appname,
                        where=server,
                        why=why
                    )
                )
            else:
                appevents.post(
                    self.events_dir,
                    traceevents.PendingTraceEvent(
                        instanceid=appname,
                        why=why
                    )
                )

    def _abort_task(self, appname, exception):
        """Set task into aborted state in case of scheduling error."""
        if self.events_dir:
            appevents.post(
                self.events_dir,
                traceevents.AbortedTraceEvent(
                    instanceid=appname,
                    why=app_abort.SCHEDULER,
                    payload=exception
                )
            )

    def remove_app(self, appname):
        """Remove app from scheduler."""
        if appname not in self.cell.apps:
            return

        app = self.cell.apps[appname]

        if app.server:
            self.backend.delete(z.path.placement(app.server, appname))

        if self.events_dir:
            appevents.post(
                self.events_dir,
                traceevents.DeletedTraceEvent(
                    instanceid=appname
                )
            )

        # If finished does not exist, it means app is terminated by
        # explicit request, not because it finished on the node.
        if not self.backend.exists(z.path.finished(appname)):
            self.backend.put(
                z.path.finished(appname),
                {'state': 'terminated',
                 'when': time.time(),
                 'host': app.server,
                 'data': None},
            )

        super(Master, self).remove_app(appname)

    def _calculate_aggregate(self, apps):
        """Calculate aggregate # of apps by proid."""
        aggregate = collections.Counter()
        for app in apps:
            aggregate[app[:app.find('.')]] += 1
        return dict(aggregate)
