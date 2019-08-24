package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/kustomize/internal/tools/crawler"
	"sigs.k8s.io/kustomize/internal/tools/crawler/github"
	"sigs.k8s.io/kustomize/internal/tools/doc"
	"sigs.k8s.io/kustomize/internal/tools/httpclient"
	"sigs.k8s.io/kustomize/internal/tools/index"

	"github.com/gomodule/redigo/redis"
)

const (
	githubAccessTokenVar = "GITHUB_ACCESS_TOKEN"
	redisURL             = "REDIS_URL"
	retryCount           = 3
)

func getRepoName(repoURL string) (string, error) {
	// "1https:1/2/3github.com3/4user4/5repo5
	repo := strings.Split(repoURL, "/")
	if len(repo) < 5 {
		return "", fmt.Errorf("repo format not as expected: %+v\n", repo)
	}

	return strings.Join(repo[3:], "/"), nil
}

func updateDocuments(idx *index.KustomizeIndex, client github.GitHubClient) {
	query := []byte(`{ "query":{ "match_all":{} } }`)
	it := idx.IterateQuery(query, 10000, 60*time.Second)
	docs := make(index.KustomizeHits, 0)
	for it.Next() {
		docs = append(docs, it.Value().Hits.Hits...)
	}
	if err := it.Err(); err != nil {
		fmt.Printf("Error iterating: %v\n", err)
	}

	for _, d := range docs {
		k := d.Document
		if k.DefaultBranch == "" {
			fs := github.GithubFileSpec{}
			repo, err := getRepoName(k.RepositoryURL)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			fs.Repository.API = client.ReposRequest(repo)
			branch, err := client.GetDefaultBranch(fs)
			if err != nil {
				fmt.Printf("Error updating documents: %v", err)
				k.DefaultBranch = "master"
			} else {
				k.DefaultBranch = branch
			}
		}
		idx.Put(d.ID, &k)
	}
	return
}

func getNewDocuments(idx *index.KustomizeIndex, client github.GitHubClient) {
	wg := sync.WaitGroup{}

	docStream := make(chan *doc.KustomizationDocument, 1<<10)
	wg.Add(1)
	go func() {
		defer wg.Done()
		totalCnt := 0
		errorCnt := 0
		for doc := range docStream {
			totalCnt++
			err := doc.ParseYAML()
			if err != nil {
				docBytes, _ := json.MarshalIndent(&doc, "", "  ")
				fmt.Printf("Error: document is not valid YAML: %s\n", docBytes)
				errorCnt++
			}

			_, err = idx.Put("", doc)
			if err != nil {
				fmt.Printf("Index error: %v\n", err)
			}
		}
		fmt.Printf("Recieved %d documents, %d with invalid YAML\n", totalCnt, errorCnt)
	}()

	ctx := context.Background()
	errs := crawler.CrawlerRunner(ctx, docStream, []crawler.Crawler{})

	for _, err := range errs {
		fmt.Println("Error: ", err)
	}
	wg.Wait()
}

func getAllResources(idx *index.KustomizeIndex, client github.GitHubClient, crawler crawler.Crawler) {
	query := []byte(`{ "query":{ "match_all":{} } }`)
	it := idx.IterateQuery(query, 10000, 60*time.Second)
	docs := make(index.KustomizeHits, 0)
	for it.Next() {
		docs = append(docs, it.Value().Hits.Hits...)
	}
	if err := it.Err(); err != nil {
		fmt.Printf("Error iterating: %v\n", err)
	}

	for _, d := range docs {
		k := d.Document
		res, err := doc.GetResources(k.DocumentData)
		if err != nil {
			fmt.Printf("Error for doc(%s %s): %v\n",
				k.RepositoryURL, k.FilePath, err)
		}

		kDir, _ := path.Split(k.FilePath)
		repoName, err := getRepoName(k.RepositoryURL)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		for _, r := range res {
			if !strings.HasSuffix(r, ".yaml") && !strings.HasSuffix(r, ".json") {
				continue
			}

			rawurl := fmt.Sprintf(
				"https://raw.githubusercontent.com/%s/master/%s/%s",
				repoName, kDir, r,
			)
			fmt.Println("Rawurl: ", rawurl)
			res, err := client.GetRawUserContent(rawurl)
			if err != nil {
				fmt.Printf("Error could not get contents: %v\n", err)
			}

			defer res.Body.Close()
			data, err := ioutil.ReadAll(res.Body)
			if err != nil {
				fmt.Printf("Error could not read contents: %v\n", err)
			}
			document := doc.KustomizationDocument{
				DocumentData:  string(data),
				RepositoryURL: k.RepositoryURL,
				FilePath:      kDir + r,
			}
			err = document.ParseYAML()
			if err != nil {
				fmt.Printf("Error failed to parse file: %v\n", err)
			}

			id, err := idx.Put("", &document)
			fmt.Println("Id: ", id)
		}
	}
}

func main() {
	githubToken := os.Getenv(githubAccessTokenVar)
	if githubToken == "" {
		fmt.Printf("Must set the variable '%s' to make github requests.\n",
			githubAccessTokenVar)
		return
	}

	ctx := context.Background()
	idx, err := index.NewKustomizeIndex(ctx)
	if err != nil {
		fmt.Printf("Could not create an index: %v\n", err)
		return
	}
	rURL := os.Getenv(redisURL)
	conn, err := redis.DialURL(rURL)
	if err != nil {
		fmt.Printf("Error: redis could not make a connection: %v\n", err)
	}

	clientCache := httpclient.NewClient(conn)
	client := github.NewClient(githubToken, retryCount, clientCache)
	_ = github.NewCrawler(githubToken, retryCount, clientCache,
		github.QueryWith(
			github.Filename("kustomization.yaml"),
			github.Filename("kustomization.yml")),
	)
	// Update the existing documents.
	updateDocuments(idx, client)
	// Adds all documents to the database.
	// getNewDocuments(idx, client, crawler)
	// Updates the resources of each document.
	// getAllResources(idx, client)
}
