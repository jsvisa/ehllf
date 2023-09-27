package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Log struct {
	BlockNumber uint64
	BlockHash   string
	TxHash      string
	TxIndex     uint
	Index       uint
	Address     string
	Topics      string
	NTopics     int
	Data        string
	Removed     bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
	DeletedAt   gorm.DeletedAt
}

func (Log) TableName() string {
	return "logs"
}

func InitDB(dsn string) *gorm.DB {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Crit("failed to init gorm connection", "err", err)
	}

	l := &Log{}
	now := time.Now().Format("20060102150405")

	renameSQL := fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME TO %s_%s", l.TableName(), l.TableName(), now)
	log.Info("rename table", "sql", renameSQL)
	db.Exec(renameSQL)
	db.AutoMigrate(l)

	return db
}

var (
	provider  = flag.String("geth-ws-url", "ws://127.0.0.1:8546", "Geth websocket RPC provider")
	dbDSN     = flag.String("postgres-url", "postgres://postgres:@127.0.0.1:5432/tsdb?sslmode=disable", "PostgreSQL DSN")
	fromBlock = flag.Int("from", 1, "from block number")
	toBlock   = flag.Int("to", 0, "to block number")
	workers   = flag.Int("workers", 10, "DB workers")
	verbose   = flag.Bool("verbose", false, "in verbose mode")
)

func main() {
	flag.Parse()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stdout, log.TerminalFormat(true))))
	db := InitDB(*dbDSN)

	client, err := ethclient.Dial(*provider)
	if err != nil {
		log.Crit("error creating client", "err", err)
	}

	var (
		ctx = context.Background()
		wg  sync.WaitGroup

		logsCh   = make(chan types.Log, 10000)
		pipeline = make(chan types.Log, *workers*40000)
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		sub, err := client.Client().EthSubscribe(ctx, logsCh, "logs", map[string]interface{}{"fromBlock": hexutil.Uint64(*fromBlock).String()})
		if err != nil {
			log.Crit("failed to subscribe logs", "err", err)
		}
		defer sub.Unsubscribe()

		log.Info("Success subscribe filter logs")
		last := time.Now()
	loop:
		for {
			select {
			case e := <-logsCh:
				pipeline <- e
				if *verbose {
					ef, _ := json.MarshalIndent(e, "", "  ")
					log.Info("filtered log --->")
					fmt.Println(string(ef))
				} else if now := time.Now(); now.After(last.Add(10 * time.Second)) {
					log.Info("Log", "block", e.BlockNumber, "txIndex", e.TxIndex, "index", e.Index, "removed", e.Removed)
					last = now
				}
			case <-ctx.Done():
				log.Error("subscribe done")
				break loop
			case err := <-sub.Err():
				log.Error("subscribe error occured", "err", err)
				break loop
			}
		}
		close(logsCh)
	}()

	go func() {
		timer := time.NewTimer(8 * time.Second)
		for {
			select {
			case <-timer.C:
				log.Info(":rocket: channel size", "#pipeline", len(pipeline), "#logs", len(logsCh))
				timer.Reset(8 * time.Second)
			}
		}
	}()

	for job := 0; job < *workers; job++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for e := range pipeline {
				topics := make([]string, len(e.Topics))
				for i, t := range e.Topics {
					topics[i] = t.Hex()
				}
				l := &Log{
					BlockNumber: e.BlockNumber,
					BlockHash:   e.BlockHash.Hex(),
					TxHash:      e.TxHash.Hex(),
					TxIndex:     e.TxIndex,
					Address:     e.Address.Hex(),
					Topics:      strings.Join(topics, ","),
					NTopics:     len(e.Topics),
					Data:        hexutil.Encode(e.Data),
					Removed:     e.Removed,
				}
				if result := db.Create(l).Error; result != nil {
					log.Crit("failed to insert log", "err", "result")
				}
			}
		}()
	}

	wg.Wait()
}
