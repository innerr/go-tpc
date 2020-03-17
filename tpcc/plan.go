package tpcc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func pad(s string, max int) string {
	for len(s) < max {
		s += " "
	}
	return s
}

func PrintQueryPlan(ctx context.Context, conn *sql.Conn, query string, args ...interface{}) {
	expSql := fmt.Sprintf("EXPLAIN "+strings.Replace(query, "?", "%d", -1)+"\n", args)
	type PlanRow struct {
		Id      string
		EstRows string
		Task    string
		OpInfo  string
	}
	expRows, err := conn.QueryContext(ctx, expSql)
	if err != nil {
		print(fmt.Errorf("explan %s failed %v", query, err))
		return
	}
	plan := make([]PlanRow, 0)
	for expRows.Next() {
		var p PlanRow
		expRows.Scan(&p.Id, &p.EstRows, &p.Task, &p.OpInfo)
		plan = append(plan, p)
	}
	maxLenId := 0
	maxLenEstRows := 0
	maxLenTask := 0
	maxLenOpInfo := 0
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
		if len(plan[i].OpInfo) > maxLenOpInfo {
			maxLenOpInfo = len(plan[i].OpInfo)
		}
	}
	for _, p := range plan {
		fmt.Println(pad(p.Id, maxLenId), pad(p.EstRows, maxLenEstRows), pad(p.Task, maxLenTask), pad(p.OpInfo, maxLenOpInfo))
	}
	expRows.Close()

}
