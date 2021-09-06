# adjust

## Features

- Top 10 active users sorted by amount of PRs created and commits pushed
- Top 10 repositories sorted by amount of commits pushed
- Top 10 repositories sorted by amount of watch events


## Installation
```shell script
go build -o analyser main/main.go
```

##Usage

```shell script
./analyser -dir directoryPathToLogFiles -query queryName
```
`directoryPathToLogFiles` is the path of the directory which contains the log files.
This is an optional flag, by default it uses the sampledata directory in the project root, included in the repo. 

`queryName` is one of the supported queries
`MostActiveUser ` `MostCommittedRepos` `MostWatchedRepos`  
##Sample Runs.


```sh 
    Rohits-MacBook-Pro.local:~/go/src/github.com/adjust$ ./analyser -query MostWatchedRepos
    victorqribeiro/isocity
    neutraltone/awesome-stock-resources
    GitHubDaily/GitHubDaily
    sw-yx/spark-joy
    imsnif/bandwhich
    Chakazul/Lenia
    BurntSushi/xsv
    neeru1207/AI_Sudoku
    ErikCH/DevYouTubeList
    FiloSottile/age
    

    Rohits-MacBook-Pro.local:~/go/src/github.com/adjust$ ./analyser -dir /Users/rohitagrawal/Downloads/adj -query MostCommittedRepos
    lihkg-backup/thread
    otiny/up
    ripamf2991/ntdtv
    textileio/js-foldersync
    ripamf2991/djy
    foreign-sub/home-assistant
    himobi/hotspot
    wigforss/java-8-base
    geos4s/18w856162
    Lambda-School-Labs/conference-contacts-ios

   
    Rohits-MacBook-Pro.local:~/go/src/github.com/adjust$ ./analyser -dir /Users/rohitagrawal/Downloads/adj -query MostWatchedRepos
    victorqribeiro/isocity
    neutraltone/awesome-stock-resources
    GitHubDaily/GitHubDaily
    sw-yx/spark-joy
    imsnif/bandwhich
    BurntSushi/xsv
    Chakazul/Lenia
    neeru1207/AI_Sudoku
    ErikCH/DevYouTubeList
    FiloSottile/age
```

###Notes

1. I did not expect the language to be specified as golang for the solution, My preferred language would have been JAVA, as golang is fairly new to me.
2. Test coverage is not great because of time constraint.
3. I did it one sitting, hence a single big commit.
4. There might be edge cases which the solution does not take into account. I was instructed to not go deep into edge cases and limit the time I devote to the solution.