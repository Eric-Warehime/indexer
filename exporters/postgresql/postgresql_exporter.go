package postgresql

import (
	"fmt"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/ledger/ledgercore"
	"github.com/algorand/indexer/exporters"
	"github.com/algorand/indexer/idb"
	"github.com/algorand/indexer/idb/postgres"
	"github.com/algorand/indexer/importer"
	"github.com/algorand/indexer/plugins"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

const exporterName = "postgresql"

type postgresqlExporter struct {
	round  uint64
	cfg    ExporterConfig
	db     *postgres.IndexerDb
	logger *logrus.Logger
}

var postgresqlExporterMetadata = exporters.ExporterMetadata{
	ExpName:        exporterName,
	ExpDescription: "Exporter for writing data to a postgresql instance.",
	ExpDeprecated:  false,
}

// Constructor is the ExporterConstructor implementation for the "postgresql" exporter
type Constructor struct{}

// New initializes a postgresqlExporter
func (c *Constructor) New() exporters.Exporter {
	return &postgresqlExporter{
		round: 0,
	}
}

func (exp *postgresqlExporter) Metadata() exporters.ExporterMetadata {
	return postgresqlExporterMetadata
}

func (exp *postgresqlExporter) Connect(cfg plugins.PluginConfig, logger *logrus.Logger) error {
	exp.logger = logger
	if err := exp.unmarhshalConfig(string(cfg)); err != nil {
		return fmt.Errorf("connect failure in unmarshalConfig: %v", err)
	}
	var opts idb.IndexerDbOptions
	opts.MaxConn = exp.cfg.MaxConn
	opts.ReadOnly = false
	db, ready, err := postgres.OpenPostgres(exp.cfg.ConnectionString, opts, exp.logger)
	if err != nil {
		return fmt.Errorf("connect failure in OpenPostgres: %v", err)
	}
	exp.db = db
	<-ready
	return err
}

func (exp *postgresqlExporter) Config() plugins.PluginConfig {
	ret, _ := yaml.Marshal(exp.cfg)
	return plugins.PluginConfig(ret)
}

func (exp *postgresqlExporter) Disconnect() error {
	exp.db.Close()
	return nil
}

func (exp *postgresqlExporter) Receive(exportData exporters.ExportData) error {
	// Do we need to test for consensus protocol here?
	/*
		_, ok := config.Consensus[block.CurrentProtocol]
			if !ok {
				return fmt.Errorf("protocol %s not found", block.CurrentProtocol)
		}
	*/
	blkExpData, ok := exportData.(exporters.BlockExportData)
	if !ok {
		return fmt.Errorf("receive error, unable to convert input %#v to BlockExportData", exportData)
	}
	exp.round = exportData.Round() + 1
	vb := ledgercore.MakeValidatedBlock(blkExpData.Block, blkExpData.Delta)
	return exp.db.AddBlock(&vb)
}

func (exp *postgresqlExporter) HandleGenesis(genesis bookkeeping.Genesis) error {
	_, err := importer.EnsureInitialImport(exp.db, genesis, exp.logger)
	return err
}

func (exp *postgresqlExporter) Round() uint64 {
	// should we try to retrieve this from the db? That could fail.
	// return exp.db.GetNextRoundToAccount()
	return exp.round
}

func (exp *postgresqlExporter) unmarhshalConfig(cfg string) error {
	return yaml.Unmarshal([]byte(cfg), exp.cfg)
}

func init() {
	exporters.RegisterExporter(exporterName, &Constructor{})
}
