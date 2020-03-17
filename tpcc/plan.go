package tpcc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"unicode/utf8"
)

func pad(s string, max int) string {
	for utf8.RuneCountInString(s) < max {
		s += " "
	}
	return s
}

type PlanRow struct {
	Id      string
	EstRows string
	Task    string
	OpInfo  string
}

func PrintQueryPlan(ctx context.Context, conn *sql.Conn, query string, args ...interface{}) {
	var argStrs []interface{}
	for arg := range args {
		argStrs = append(argStrs, string(arg))
	}

	expSql := fmt.Sprintf(strings.Replace(query, "?", "%#v", -1), args...)
	fmt.Println("[" + expSql + "]")

	expRows, err := conn.QueryContext(ctx, "EXPLAIN "+expSql)
	if err != nil {
		fmt.Printf("explan %s failed %v\n", query, err.Error())
		return
	}
	defer expRows.Close()

	plan := make([]PlanRow, 0)
	for expRows.Next() {
		var p PlanRow
		expRows.Scan(&p.Id, &p.EstRows, &p.Task, &p.OpInfo)
		plan = append(plan, p)
	}

	maxLenId := 0
	maxLenEstRows := 0
	maxLenTask := 0
	for i := 0; i < len(plan); i++ {
		if len(plan[i].Id) > maxLenId {
			maxLenId = len(plan[i].Id)
		}
		if len(plan[i].EstRows) > maxLenEstRows {
			maxLenEstRows = len(plan[i].EstRows)
		}
		if len(plan[i].Task) > maxLenTask {
			maxLenTask = len(plan[i].Task)
		}
	}

	for _, p := range plan {
		fmt.Println(pad(p.Id, maxLenId), pad(p.EstRows, maxLenEstRows), pad(p.Task, maxLenTask), p.OpInfo)
	}
	fmt.Println("---")
}
