package mssqltodal

import (
	"fmt"
	"log"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
)

type ColumnResult struct {
	TableName                string `gorm:"column:Table_Name"`
	ColumnName               string `gorm:"column:Column_name"`
	IsNullable               string `gorm:"column:is_Nullable"`
	Default                  string `gorm:"column:COLUMN_DEFAULT"`
	DataType                 string `gorm:"column:data_type"`
	CONSTRAINT_NAME          string `gorm:"column:CONSTRAINT_NAME"`
	CHARACTER_MAXIMUM_LENGTH int    `gorm:"column:CHARACTER_MAXIMUM_LENGTH;"`
	NUMERIC_PRECISION        string `gorm:"column:NUMERIC_PRECISION"`
	NUMERIC_SCALE            string `gorm:"column:NUMERIC_SCALE"`
}

type TableDalDetails struct {
	TableName     string
	ColumnDetails []ColumnResult
	PrimaryKeys   []string
}

/* Fix primary keys */
func (T *TableDalDetails) AddTable(table []ColumnResult) {

	for _, t := range table {
		if "PK_"+t.TableName == t.CONSTRAINT_NAME {
			T.PrimaryKeys = append(T.PrimaryKeys, t.ColumnName)
		}
	}

	T.TableName = table[0].TableName
	T.ColumnDetails = table

}

type FinalTablesDetails []TableDalDetails

var dataMapping = make(map[string]string)

func fixData(d []string) string {

	returnString := "["
	for index, s := range d {

		returnString += "'" + s + "'"
		if index < len(d)-1 {
			returnString += ","
		}
	}
	returnString += "]"
	return returnString
}

func (T FinalTablesDetails) PrintDalTables(dbUser, dbPass, server, port, dbName string) {

	dataMapping["bigint"] = "bigint"
	dataMapping["int"] = "integer"
	dataMapping["smallint"] = "integer"
	dataMapping["tinyint"] = "integer"
	dataMapping["decimal"] = "decimal"
	dataMapping["numeric"] = "decimal"
	dataMapping["float"] = "double"
	dataMapping["real"] = "double"
	dataMapping["datetime"] = "datetime"
	dataMapping["char"] = "string"
	dataMapping["varchar"] = "text"
	dataMapping["text"] = "text"
	dataMapping["nvarchar"] = "text"
	dataMapping["bit"] = "integer"

	/*
		mssqldb=DAL('mssql://host:host@192.168.1.3/HOST_IMPEXP',driver_args = {'DRIVER' :  '/opt/microsoft/msodbcsql/lib64/libmsodbcsql-13.1.so.9.2'}, fake_migrate=True)
	*/

	connectionString := fmt.Sprintf("mssqldb=DAL('mssql://%s:%s@%s/%s',fake_migrate=True)", dbUser, dbPass, server, dbName)
	fmt.Printf("%s\n\n", connectionString)

	for _, t := range T {

		fmt.Printf("mssqldb.define_table('%s',\n", t.TableName)
		for _, c := range t.ColumnDetails {
			var dataType = dataMapping[c.DataType]
			if dataType == "decimal" {
				dataType += "(" + c.NUMERIC_PRECISION + "," + c.NUMERIC_SCALE + ")"
			}
			fmt.Printf("\tField('%s','%s', rname='%s'),\n", c.ColumnName, dataType, c.ColumnName)

		}
		fmt.Printf("\tprimarykey=%s,\n", fixData(t.PrimaryKeys))
		fmt.Printf("\trname='%s.dbo.%s')\n\n", dbName, t.TableName)
	}

}

type Result struct {
	Name string
}

var sqlColumns []ColumnResult
var sqlTables []Result

var DalTables FinalTablesDetails

func (T FinalTablesDetails) Run(dbUser, dbPass, server, port, dbName string) {

	var connectionString = fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s", dbUser, dbPass, server, port, dbName)

	db, err := gorm.Open("mssql", connectionString)
	if err != nil {
		panic("failed to connect database")
	}

	db.Raw("SELECT * from sys.tables").Scan(&sqlTables)
	log.Printf("FOUND %v TABLES \n", len(sqlTables))

	for _, tableName := range sqlTables {
		log.Printf("NEW TABLE FOUND : %s \n", tableName.Name)
		var query = fmt.Sprintf(" select C.Table_Name, C.Column_name, data_type, is_Nullable, U.CONSTRAINT_NAME, CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE from information_Schema.Columns C FULL OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE U ON C.COLUMN_NAME = U.COLUMN_NAME WHERE C.TABLE_NAME='%s';", tableName.Name)
		db.Raw(query).Scan(&sqlColumns)
		var tablesToDal TableDalDetails
		tablesToDal.AddTable(sqlColumns)
		DalTables = append(DalTables, tablesToDal)

	}

	DalTables.PrintDalTables(dbUser, dbPass, server, port, dbName)

}
