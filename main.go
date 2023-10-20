package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	parserformat "github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/tidb"
	field_types "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/stringutil"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please specify the file path.")
		os.Exit(1)
	}
	path := os.Args[1]
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	tmp := &model.TableInfo{}
	err = json.Unmarshal(data, tmp)
	if err != nil {
		fmt.Println(err)
		return
	}
	var b bytes.Buffer
	err = ConstructResultOfShowCreateTable(tmp, &b)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(b.String())
}

func ConstructResultOfShowCreateTable(tableInfo *model.TableInfo, buf *bytes.Buffer) (err error) {
	tblCharset := tableInfo.Charset
	if len(tblCharset) == 0 {
		tblCharset = mysql.DefaultCharset
	}
	tblCollate := tableInfo.Collate

	sqlMode, _ := mysql.GetSQLMode(mysql.DefaultSQLMode)
	tableName := stringutil.Escape(tableInfo.Name.O, sqlMode)
	switch tableInfo.TempTableType {
	case model.TempTableGlobal:
		fmt.Fprintf(buf, "CREATE GLOBAL TEMPORARY TABLE %s (\n", tableName)
	case model.TempTableLocal:
		fmt.Fprintf(buf, "CREATE TEMPORARY TABLE %s (\n", tableName)
	default:
		fmt.Fprintf(buf, "CREATE TABLE %s (\n", tableName)
	}
	var pkCol *model.ColumnInfo
	needAddComma := false
	for i, col := range tableInfo.Cols() {
		if col.Hidden {
			continue
		}
		if needAddComma {
			buf.WriteString(",\n")
		}
		fmt.Fprintf(buf, "  %s %s", stringutil.Escape(col.Name.O, sqlMode), col.GetTypeDesc())
		if field_types.HasCharset(&col.FieldType) {
			if col.GetCharset() != tblCharset {
				fmt.Fprintf(buf, " CHARACTER SET %s", col.GetCharset())
			}
			if col.GetCollate() != tblCollate {
				fmt.Fprintf(buf, " COLLATE %s", col.GetCollate())
			} else {
				defcol, err := charset.GetDefaultCollation(col.GetCharset())
				if err == nil && defcol != col.GetCollate() {
					fmt.Fprintf(buf, " COLLATE %s", col.GetCollate())
				}
			}
		}
		if col.IsGenerated() {
			// It's a generated column.
			fmt.Fprintf(buf, " GENERATED ALWAYS AS (%s)", col.GeneratedExprString)
			if col.GeneratedStored {
				buf.WriteString(" STORED")
			} else {
				buf.WriteString(" VIRTUAL")
			}
		}
		if mysql.HasAutoIncrementFlag(col.GetFlag()) {
			buf.WriteString(" NOT NULL AUTO_INCREMENT")
		} else {
			if mysql.HasNotNullFlag(col.GetFlag()) {
				buf.WriteString(" NOT NULL")
			}
			// default values are not shown for generated columns in MySQL
			if !mysql.HasNoDefaultValueFlag(col.GetFlag()) && !col.IsGenerated() {
				defaultValue := col.GetDefaultValue()
				switch defaultValue {
				case nil:
					if !mysql.HasNotNullFlag(col.GetFlag()) {
						if col.GetType() == mysql.TypeTimestamp {
							buf.WriteString(" NULL")
						}
						buf.WriteString(" DEFAULT NULL")
					}
				case "CURRENT_TIMESTAMP", "CURRENT_DATE":
					buf.WriteString(" DEFAULT ")
					buf.WriteString(defaultValue.(string))
					if col.GetDecimal() > 0 {
						buf.WriteString(fmt.Sprintf("(%d)", col.GetDecimal()))
					}
				default:
					defaultValStr := fmt.Sprintf("%v", defaultValue)
					//// If column is timestamp, and default value is not current_timestamp, should convert the default value to the current session time zone.
					//if col.GetType() == mysql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr {
					//	timeValue, err := table.GetColDefaultValue(ctx, col)
					//	if err != nil {
					//		return errors.Trace(err)
					//	}
					//	defaultValStr = timeValue.GetMysqlTime().String()
					//}

					if col.DefaultIsExpr {
						fmt.Fprintf(buf, " DEFAULT %s", format.OutputFormat(defaultValStr))
					} else {
						if col.GetType() == mysql.TypeBit {
							defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
							fmt.Fprintf(buf, " DEFAULT %s", defaultValBinaryLiteral.ToBitLiteralString(true))
						} else {
							fmt.Fprintf(buf, " DEFAULT '%s'", format.OutputFormat(defaultValStr))
						}
					}
				}
			}
			if mysql.HasOnUpdateNowFlag(col.GetFlag()) {
				buf.WriteString(" ON UPDATE CURRENT_TIMESTAMP")
				buf.WriteString(table.OptionalFsp(&col.FieldType))
			}
		}
		if ddl.IsAutoRandomColumnID(tableInfo, col.ID) {
			s, r := tableInfo.AutoRandomBits, tableInfo.AutoRandomRangeBits
			if r == 0 || r == autoid.AutoRandomRangeBitsDefault {
				buf.WriteString(fmt.Sprintf(" /*T![auto_rand] AUTO_RANDOM(%d) */", s))
			} else {
				buf.WriteString(fmt.Sprintf(" /*T![auto_rand] AUTO_RANDOM(%d, %d) */", s, r))
			}
		}
		if len(col.Comment) > 0 {
			buf.WriteString(fmt.Sprintf(" COMMENT '%s'", format.OutputFormat(col.Comment)))
		}
		if i != len(tableInfo.Cols())-1 {
			needAddComma = true
		}
		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()) {
			pkCol = col
		}
	}

	if pkCol != nil {
		// If PKIsHandle, pk info is not in tb.Indices(). We should handle it here.
		buf.WriteString(",\n")
		fmt.Fprintf(buf, "  PRIMARY KEY (%s)", stringutil.Escape(pkCol.Name.O, sqlMode))
		buf.WriteString(" /*T![clustered_index] CLUSTERED */")
	}

	publicIndices := make([]*model.IndexInfo, 0, len(tableInfo.Indices))
	for _, idx := range tableInfo.Indices {
		if idx.State == model.StatePublic {
			publicIndices = append(publicIndices, idx)
		}
	}
	if len(publicIndices) > 0 {
		buf.WriteString(",\n")
	}

	for i, idxInfo := range publicIndices {
		if idxInfo.Primary {
			buf.WriteString("  PRIMARY KEY ")
		} else if idxInfo.Unique {
			fmt.Fprintf(buf, "  UNIQUE KEY %s ", stringutil.Escape(idxInfo.Name.O, sqlMode))
		} else {
			fmt.Fprintf(buf, "  KEY %s ", stringutil.Escape(idxInfo.Name.O, sqlMode))
		}

		cols := make([]string, 0, len(idxInfo.Columns))
		var colInfo string
		for _, c := range idxInfo.Columns {
			if tableInfo.Columns[c.Offset].Hidden {
				colInfo = fmt.Sprintf("(%s)", tableInfo.Columns[c.Offset].GeneratedExprString)
			} else {
				colInfo = stringutil.Escape(c.Name.O, sqlMode)
				if c.Length != types.UnspecifiedLength {
					colInfo = fmt.Sprintf("%s(%s)", colInfo, strconv.Itoa(c.Length))
				}
			}
			cols = append(cols, colInfo)
		}
		fmt.Fprintf(buf, "(%s)", strings.Join(cols, ","))
		if idxInfo.Invisible {
			fmt.Fprintf(buf, ` /*!80000 INVISIBLE */`)
		}
		if idxInfo.Comment != "" {
			fmt.Fprintf(buf, ` COMMENT '%s'`, format.OutputFormat(idxInfo.Comment))
		}
		if idxInfo.Primary {
			if tableInfo.HasClusteredIndex() {
				buf.WriteString(" /*T![clustered_index] CLUSTERED */")
			} else {
				buf.WriteString(" /*T![clustered_index] NONCLUSTERED */")
			}
		}
		if i != len(publicIndices)-1 {
			buf.WriteString(",\n")
		}
	}

	// Foreign Keys are supported by data dictionary even though
	// they are not enforced by DDL. This is still helpful to applications.
	for _, fk := range tableInfo.ForeignKeys {
		buf.WriteString(fmt.Sprintf(",\n  CONSTRAINT %s FOREIGN KEY ", stringutil.Escape(fk.Name.O, sqlMode)))
		colNames := make([]string, 0, len(fk.Cols))
		for _, col := range fk.Cols {
			colNames = append(colNames, stringutil.Escape(col.O, sqlMode))
		}
		buf.WriteString(fmt.Sprintf("(%s)", strings.Join(colNames, ",")))
		if fk.RefSchema.L != "" {
			buf.WriteString(fmt.Sprintf(" REFERENCES %s.%s ", stringutil.Escape(fk.RefSchema.O, sqlMode), stringutil.Escape(fk.RefTable.O, sqlMode)))
		} else {
			buf.WriteString(fmt.Sprintf(" REFERENCES %s ", stringutil.Escape(fk.RefTable.O, sqlMode)))
		}
		refColNames := make([]string, 0, len(fk.Cols))
		for _, refCol := range fk.RefCols {
			refColNames = append(refColNames, stringutil.Escape(refCol.O, sqlMode))
		}
		buf.WriteString(fmt.Sprintf("(%s)", strings.Join(refColNames, ",")))
		if model.ReferOptionType(fk.OnDelete) != 0 {
			buf.WriteString(fmt.Sprintf(" ON DELETE %s", model.ReferOptionType(fk.OnDelete).String()))
		}
		if model.ReferOptionType(fk.OnUpdate) != 0 {
			buf.WriteString(fmt.Sprintf(" ON UPDATE %s", model.ReferOptionType(fk.OnUpdate).String()))
		}
		if fk.Version < model.FKVersion1 {
			buf.WriteString(" /* FOREIGN KEY INVALID */")
		}
	}

	buf.WriteString("\n")

	buf.WriteString(") ENGINE=InnoDB")
	// We need to explicitly set the default charset and collation
	// to make it work on MySQL server which has default collate utf8_general_ci.
	if len(tblCollate) == 0 || tblCollate == "binary" {
		// If we can not find default collate for the given charset,
		// or the collate is 'binary'(MySQL-5.7 compatibility, see #15633 for details),
		// do not show the collate part.
		fmt.Fprintf(buf, " DEFAULT CHARSET=%s", tblCharset)
	} else {
		fmt.Fprintf(buf, " DEFAULT CHARSET=%s COLLATE=%s", tblCharset, tblCollate)
	}

	// Displayed if the compression typed is set.
	if len(tableInfo.Compression) != 0 {
		fmt.Fprintf(buf, " COMPRESSION='%s'", tableInfo.Compression)
	}

	if tableInfo.AutoIdCache != 0 {
		fmt.Fprintf(buf, " /*T![auto_id_cache] AUTO_ID_CACHE=%d */", tableInfo.AutoIdCache)
	}

	if tableInfo.ShardRowIDBits > 0 {
		fmt.Fprintf(buf, " /*T! SHARD_ROW_ID_BITS=%d ", tableInfo.ShardRowIDBits)
		if tableInfo.PreSplitRegions > 0 {
			fmt.Fprintf(buf, "PRE_SPLIT_REGIONS=%d ", tableInfo.PreSplitRegions)
		}
		buf.WriteString("*/")
	}

	if len(tableInfo.Comment) > 0 {
		fmt.Fprintf(buf, " COMMENT='%s'", format.OutputFormat(tableInfo.Comment))
	}

	if tableInfo.TempTableType == model.TempTableGlobal {
		fmt.Fprintf(buf, " ON COMMIT DELETE ROWS")
	}

	if tableInfo.PlacementPolicyRef != nil {
		fmt.Fprintf(buf, " /*T![placement] PLACEMENT POLICY=%s */", stringutil.Escape(tableInfo.PlacementPolicyRef.Name.String(), sqlMode))
	}

	if tableInfo.TableCacheStatusType == model.TableCacheStatusEnable {
		// This is not meant to be understand by other components, so it's not written as /*T![cached] */
		// For all external components, cached table is just a normal table.
		fmt.Fprintf(buf, " /* CACHED ON */")
	}

	if tableInfo.TTLInfo != nil {
		restoreFlags := parserformat.RestoreStringSingleQuotes | parserformat.RestoreNameBackQuotes | parserformat.RestoreTiDBSpecialComment
		restoreCtx := parserformat.NewRestoreCtx(restoreFlags, buf)

		restoreCtx.WritePlain(" ")
		err = restoreCtx.WriteWithSpecialComments(tidb.FeatureIDTTL, func() error {
			columnName := ast.ColumnName{Name: tableInfo.TTLInfo.ColumnName}
			timeUnit := ast.TimeUnitExpr{Unit: ast.TimeUnitType(tableInfo.TTLInfo.IntervalTimeUnit)}
			restoreCtx.WriteKeyWord("TTL")
			restoreCtx.WritePlain("=")
			restoreCtx.WriteName(columnName.String())
			restoreCtx.WritePlainf(" + INTERVAL %s ", tableInfo.TTLInfo.IntervalExprStr)
			return timeUnit.Restore(restoreCtx)
		})

		if err != nil {
			return err
		}

		restoreCtx.WritePlain(" ")
		err = restoreCtx.WriteWithSpecialComments(tidb.FeatureIDTTL, func() error {
			restoreCtx.WriteKeyWord("TTL_ENABLE")
			restoreCtx.WritePlain("=")
			if tableInfo.TTLInfo.Enable {
				restoreCtx.WriteString("ON")
			} else {
				restoreCtx.WriteString("OFF")
			}
			return nil
		})

		if err != nil {
			return err
		}

		restoreCtx.WritePlain(" ")
		err = restoreCtx.WriteWithSpecialComments(tidb.FeatureIDTTL, func() error {
			restoreCtx.WriteKeyWord("TTL_JOB_INTERVAL")
			restoreCtx.WritePlain("=")
			if len(tableInfo.TTLInfo.JobInterval) == 0 {
				restoreCtx.WriteString(model.DefaultJobInterval.String())
			} else {
				restoreCtx.WriteString(tableInfo.TTLInfo.JobInterval)
			}
			return nil
		})

		if err != nil {
			return err
		}
	}
	return nil
}
