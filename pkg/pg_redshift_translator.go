package rdapp

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

type PgRedshiftTranslator interface {
	TranslateToRedshiftQuery(pgQuery string) string
	TranslateToRedshiftQueryParams(pgParams []string) []types.SqlParameter
	TranslateColumnMetaDataToPgFormat(rdappCtx RdappContext, columnMetadata []types.ColumnMetadata) (wire.Columns, error)
	TranslateRowToPgFormat(rdappCtx RdappContext, redshiftRow []types.Field) ([]any, error)
}

type pgRedshiftTranslator struct {
}

func NewPgRedshiftTranslator() PgRedshiftTranslator {
	return &pgRedshiftTranslator{}
}

func (translator *pgRedshiftTranslator) TranslateToRedshiftQuery(query string) string {
	return strings.ReplaceAll(query, "$", ":")
}

func (translator *pgRedshiftTranslator) TranslateToRedshiftQueryParams(pgParams []string) []types.SqlParameter {
	var sqlParameters []types.SqlParameter
	for i, parameter := range pgParams {
		sqlParameters = append(sqlParameters, types.SqlParameter{
			Name:  aws.String(strconv.Itoa(i + 1)),
			Value: aws.String(parameter),
		})
	}
	return sqlParameters
}

func (translator *pgRedshiftTranslator) TranslateRowToPgFormat(rdappCtx RdappContext, redshiftRow []types.Field) ([]any, error) {
	var row []any
	for _, recordCol := range redshiftRow {
		switch t := recordCol.(type) {
		case *types.FieldMemberIsNull:
			row = append(row, nil)
		case *types.FieldMemberBlobValue:
			row = append(row, t.Value)
		case *types.FieldMemberBooleanValue:
			row = append(row, t.Value)
		case *types.FieldMemberDoubleValue:
			row = append(row, t.Value)
		case *types.FieldMemberLongValue:
			row = append(row, t.Value)
		case *types.FieldMemberStringValue:
			row = append(row, t.Value)
		default:
			rdappCtx.logger.Error("unknown row column format", zap.Any("recordColType", t))
			return nil, fmt.Errorf("unknown row column format %v", t)
		}
	}
	return row, nil
}

func (translator *pgRedshiftTranslator) TranslateColumnMetaDataToPgFormat(rdappCtx RdappContext, columnMetadata []types.ColumnMetadata) (wire.Columns, error) {
	var wireColumns wire.Columns
	for _, column := range columnMetadata {
		postgresType, err := translator.convertRedshiftResultTypeToPostgresType(*column.TypeName, rdappCtx.logger)
		if err != nil {
			return nil, err
		}
		wireColumns = append(wireColumns, wire.Column{
			Name:  *column.Name,
			Oid:   postgresType,
			Width: int16(column.Length),
		})
	}
	return wireColumns, nil
}

const (
	RedshiftTypeSuper       = "super"
	RedshiftTypeBool        = "bool"
	RedshiftTypeChar        = "char"
	RedshiftTypeVarchar     = "varchar"
	RedshiftTypeBpchar      = "bpchar"
	RedshiftTypeTimestamp   = "timestamp"
	RedshiftTypeTimestamptz = "timestamptz"
	RedshiftTypeFloat4      = "float4"
	RedshiftTypeFloat8      = "float8"
	RedshiftTypeInt2        = "int2"
	RedshiftTypeInt4        = "int4"
	RedshiftTypeInt8        = "int8"
	RedshiftTypeNumeric     = "numeric"
	RedshiftTypeName        = "name"
	RedshiftTypeOid         = "oid"
	RedshiftTypeAclitem     = "_aclitem"
	RedshiftTypeText        = "_text"
)

func (translator *pgRedshiftTranslator) convertRedshiftResultTypeToPostgresType(redshiftTypeName string, loggerWithContext *zap.Logger) (oid.Oid, error) {
	typeConversions := map[string]oid.Oid{
		RedshiftTypeSuper: oid.T_json,
		RedshiftTypeBool:  oid.T_bool,
		// Character types
		RedshiftTypeChar:    oid.T_varchar,
		RedshiftTypeVarchar: oid.T_varchar,
		RedshiftTypeBpchar:  oid.T_bpchar,
		RedshiftTypeText:    oid.T_text,
		// Timestamp types
		RedshiftTypeTimestamp:   oid.T_timestamp,
		RedshiftTypeTimestamptz: oid.T_timestamptz,
		// Numeric types
		RedshiftTypeFloat4:  oid.T_float4,
		RedshiftTypeFloat8:  oid.T_float8,
		RedshiftTypeInt2:    oid.T_int2,
		RedshiftTypeInt4:    oid.T_int4,
		RedshiftTypeInt8:    oid.T_int8,
		RedshiftTypeNumeric: oid.T_numeric,
		//	Esoteric types
		RedshiftTypeName:    oid.T_name,
		RedshiftTypeOid:     oid.T_oid,
		RedshiftTypeAclitem: oid.T_aclitem,
	}
	value, exists := typeConversions[redshiftTypeName]
	if !exists {
		loggerWithContext.Error("no convertor found for redshift type",
			zap.String("redshiftTypeName", redshiftTypeName))
		return 0, fmt.Errorf("no convertor found redshiftTypeName=%v", redshiftTypeName)
	}
	return value, nil
}
