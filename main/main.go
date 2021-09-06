package main

import (
	"flag"
	"fmt"
	df "github.com/adjust/dataframe"
	q "github.com/adjust/query"
	tf "github.com/adjust/transformer"
	"os"
	"path/filepath"
	"runtime"
)

var (
	_, b, _, _ = runtime.Caller(0)

	// Root folder of this project
	basepath   = filepath.Join(filepath.Dir(b), "..")

)


func main() {
	dirPath := flag.String("dir", basepath+"/sampledata/", "The path to the base directory where the log files are")
	queryType := flag.String("query", "NoQuery", "The query to execute , Possible options are 1. MostActiveUser - Top 10 active users sorted by amount of PRs created and commits pushed\n" +
		"MostCommittedRepos - Top 10 repositories sorted by amount of commits pushed\n" +
		"MostWatchedRepos - Top 10 repositories sorted by amount of watch events\n")
	flag.Parse()

	switch *queryType {
	case "MostActiveUser":
		GetMostActiveUser(dirPath)
	case "MostCommittedRepos":
		GetMostCommitedRepos(dirPath)
	case "MostWatchedRepos":
		GetMostWatchedRepos(dirPath)
	case "NoQuery":
		fmt.Println("Query param not provided: Exiting")
		os.Exit(1)
	default:
		fmt.Println("Unsupported Query: Exiting")
		os.Exit(1)
	}
}

func GetMostCommitedRepos(dirPath *string) {
	reposFile := *dirPath + "/repos.csv"
	eventsFile := *dirPath + "/events.csv"
	commitsFile := *dirPath + "/commits.csv"
	dfEvents, _ := df.LocalCsvFileLoader{FileName: eventsFile}.Load()
	dfCommits, _ := df.LocalCsvFileLoader{FileName: commitsFile}.Load()
	dfRepos, _ := df.LocalCsvFileLoader{FileName: reposFile}.Load()

	// Join events with commits to get all commits for push events as there can be multiple commits for one pushEvent.
	joiner := tf.Joiner{
		LeftDF:          *dfCommits,
		RightDF:         *dfEvents,
		JoinColumnLeft:  3,
		JoinColumnRight: 1,
	}

	joinedDf, _ := joiner.Transform()
	//Check documentation for Top10BasedOnColumnFreq
	dfMostCommitedRepos, err  := q.Top10BasedOnColumnFreq(joinedDf,7, map[string]bool{"PushEvent": true},5 )
	if err != nil{
		fmt.Println(err)
	}
	// Join with repos dataframe to get repo name for repo id.
	joiner = tf.Joiner{
		LeftDF:          *dfMostCommitedRepos,
		RightDF:         *dfRepos,
		JoinColumnLeft:  1,
		JoinColumnRight: 1,
	}

	dfJoinedWithRepos, _ := joiner.Transform()
	printCol(dfJoinedWithRepos, 4)
}


func GetMostWatchedRepos(dirPath *string){
	reposFile := *dirPath + "/repos.csv"
	eventsFile := *dirPath + "/events.csv"
	dfEvents, _ := df.LocalCsvFileLoader{FileName: eventsFile}.Load()
	dfRepos, _ := df.LocalCsvFileLoader{FileName: reposFile}.Load()
	dfMostWatchedRepos, err  := q.Top10BasedOnColumnFreq(*dfEvents,4, map[string]bool{"WatchEvent": true},2 )
	if err != nil{
		fmt.Println(err)
	}
	//Join with repos dataframe to get repo name for repo id.
	joiner := tf.Joiner{
		LeftDF:          *dfMostWatchedRepos,
		RightDF:         *dfRepos,
		JoinColumnLeft:  1,
		JoinColumnRight: 1,
	}
	dfJoinedWithRepos, _ := joiner.Transform()
	printCol(dfJoinedWithRepos, 4)
}

func GetMostActiveUser(dirPath *string){
	actorsFile := *dirPath + "/actors.csv"
	eventsFile := *dirPath + "/events.csv"
	dfEvents, _ := df.LocalCsvFileLoader{FileName: eventsFile}.Load()
	dfMostActive, err := q.Top10BasedOnColumnFreq(*dfEvents, 3, map[string]bool{"PushEvent": true, "PullRequestEvent": true}, 2)
	if err != nil{
		fmt.Println(err)
	}
	dfActors, _ := df.LocalCsvFileLoader{FileName: actorsFile}.Load()
	//Join with actor dataframe to get user name for user id.
	joiner := tf.Joiner{
		LeftDF:          *dfMostActive,
		RightDF:         *dfActors,
		JoinColumnLeft:  1,
		JoinColumnRight: 1,
	}
	dfJoined, _ := joiner.Transform()


	printCol(dfJoined, 4)
}

func printCol(dataframe df.Dataframe, colPosition int) {
	for _, row := range dataframe.Rows {
		col, _ := row.GetField(colPosition)
		fmt.Println(col.(string))
	}
}
