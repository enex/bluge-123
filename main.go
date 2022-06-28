package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis/lang/en"
	"github.com/blugelabs/bluge/index"
	"github.com/brianvoe/gofakeit/v6"
)

func main() {
	config := NewBlugeConfigWithCustomDir("data")
	config.Logger = log.New(os.Stdout, "bluge", log.Ltime)
	config.DefaultSearchAnalyzer = en.NewAnalyzer()
	writer, err := bluge.OpenWriter(config)
	if err != nil {
		log.Fatal(err)
	}
	f := gofakeit.New(1000)
	ctx, done := context.WithCancel(context.Background())
	defer done()
	go func() {
		f := gofakeit.New(1000)
		r, err := writer.Reader()
		if err != nil {
			log.Fatal(err)
		}
		for {
			q := bluge.NewBooleanQuery()
			name := f.Word()
			q.AddMust(bluge.NewMatchQuery(name).SetField("text"))
			log.Printf("Searching for %s", name)
			iter, err := r.Search(ctx, bluge.NewTopNSearch(100, q))
			if err != nil {
				log.Fatal(err)
			}
			for dm, err := iter.Next(); err == nil && dm != nil; dm, err = iter.Next() {
				log.Printf("%s", dm.String())
			}
			log.Printf("found %v", iter.Aggregations().Count())
			time.Sleep(time.Second)
		}
	}()
	for i := 0; i < 100; i++ {
		batch := bluge.NewBatch()
		for j := 0; j < 1000; j++ {
			batch.Insert(bluge.NewDocument(fmt.Sprintf("%d", i)).
				AddField(bluge.NewTextField("name", f.Name()).Aggregatable()).
				AddField(bluge.NewTextField("body", f.Paragraph(12, 5, 12, "\n\n")).StoreValue().
					HighlightMatches().WithAnalyzer(config.DefaultSearchAnalyzer).Aggregatable()))
		}
		start := time.Now()
		err := writer.Batch(batch)
		if err != nil {
			log.Fatal(err, "failed to write batch")
		}
		log.Printf("wrote batch %d in %s", i, time.Since(start))

		time.Sleep(time.Millisecond * 25)
	}
	done()
}

type DirWrapper struct {
	index.Directory
	path string
}

func (d *DirWrapper) fileName(kind string, id uint64) string {
	return fmt.Sprintf("%012x", id) + kind
}

func (d *DirWrapper) Remove(kind string, id uint64) error {
	err := d.Directory.Remove(kind, id)
	for i := 0; err != nil && i < 10; i++ {
		time.Sleep(time.Millisecond)
		segmentPath := filepath.Join(d.path, d.fileName(kind, id))
		err = os.Remove(segmentPath)
	}
	return err
}

func NewBlugeConfigWithCustomDir(path string) bluge.Config {
	return bluge.DefaultConfigWithDirectory(func() index.Directory {
		return &DirWrapper{index.NewFileSystemDirectory(path), path}
	})
}
