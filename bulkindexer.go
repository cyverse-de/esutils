package esutils

import (
	"context"

	"go.opentelemetry.io/otel"
	"gopkg.in/olivere/elastic.v5"
)

const otelName = "github.com/cyverse-de/esutils"

type BulkIndexer struct {
	es          *elastic.Client
	bulkSize    int
	bulkService *elastic.BulkService
	context     context.Context
}

func NewBulkIndexerContext(context context.Context, es *elastic.Client, bulkSize int) *BulkIndexer {
	return &BulkIndexer{bulkSize: bulkSize, es: es, bulkService: es.Bulk(), context: context}
}

func NewBulkIndexer(es *elastic.Client, bulkSize int) *BulkIndexer {
	return NewBulkIndexerContext(context.Background(), es, bulkSize)
}

func (b *BulkIndexer) Add(r elastic.BulkableRequest) error {
	_, span := otel.Tracer(otelName).Start(b.context, "BulkIndexer.Add")
	b.bulkService.Add(r)
	span.End()

	if b.bulkService.NumberOfActions() >= b.bulkSize {
		err := b.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *BulkIndexer) CanFlush() bool {
	return b.bulkService.NumberOfActions() > 0
}

func (b *BulkIndexer) Flush() error {
	ctx, span := otel.Tracer(otelName).Start(b.context, "BulkIndexer.Flush")
	defer span.End()

	_, err := b.bulkService.Do(ctx)
	if err != nil {
		return err
	}

	b.bulkService = b.es.Bulk()

	return nil
}
