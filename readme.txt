Git is a distributed version control system.

Git is free software distributed under GPL.

＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊

Git 跟踪修改　而不跟踪文件(add只是将修改提交到暂存区，提交只将暂存区的修改提交到master分支)

＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊
１．初始化一个Git仓库　使用git init命令
２．添加文件到仓库　分两步：
	第一　使用命令git add <filename1> <filename2>...该命令可反复使用多次
	第二　使用命令git commit，后可跟参数-m "****(说明信息)"。
＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊
３．在修改文件后，直至添加到仓库的过程中，我们随时可使用git status命令查看工作区的状态。（告诉你修改是否添加，是否提交，是否提交完成）
４．若使用git status得知存在修改后，可使用git diff命令查看修改了什么，再决定是否继续修改或提交。
＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊
５．使用git log命令可查看所有的提交commit记录（会给出提交相关信息），可选的参数有--pretty=oneline,使得每条提交信息只打印在一行。
６．若想回退到之前的版本，可使用git reset --hard ***命令。其中***标示你想回退到哪个版本，可直接使用git log命令查看对应版本的版本号，或者使用指针HEAD,它指向当前版本，若想回退一层使用HEAD^或HEAD~1，想回退两层HEAD^^或HEAD~2，以此类推。当然回退之后若后悔，可再指定版本号，“回退”到想去的那个版本。
７．若电脑重启，再使用git log命令则不能显示之前的提交记录。这是可使用git reflog命令，该命令将显示所有的历史操作及相关信息，从中我们可得到commit id。
＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊
８．在工作区，我们对文件修改之后，若想撤销更改可用“git checkout -- 文件名”命令，该命令可用git status命令查看有帮助信息。
    若修改后执行了add，想要撤销更改可用“git reset HEAD 文件名”命令，该命令同样可由git status命令获得相关信息。之后便回到了上面的情况。
　　若修改后add了并commit了，直接版本回退即可，参考６．
