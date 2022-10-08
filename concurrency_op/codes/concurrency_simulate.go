package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// +---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
// | Table   | Create Table                                                                                                                                                                                              |
// +---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
// | persons | CREATE TABLE `persons` (
//   `id` int(11) NOT NULL AUTO_INCREMENT,
//   `age` int(11) DEFAULT NULL,
//   `sum` int(11) DEFAULT NULL,
//   PRIMARY KEY (`id`)
// ) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1 |
// +---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

// +--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
// | Table        | Create Table                                                                                                                                                                                                                                                                                                                                                                        |
// +--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
// | person_infos | CREATE TABLE `person_infos` (
//   `id` int(11) NOT NULL AUTO_INCREMENT,
//   `person_id` int(11) DEFAULT NULL,
//   `sub` int(11) DEFAULT NULL,
//   `info` varchar(255) DEFAULT NULL,
//   PRIMARY KEY (`id`),
//   KEY `person_id` (`person_id`),
//   CONSTRAINT `person_infos_ibfk_1` FOREIGN KEY (`person_id`) REFERENCES `persons` (`id`)
// ) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=latin1 |
// +--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

type MyHandler struct {
	db                 *sql.DB
	max_sub_num        int // one person record can have max number of records in person_info.
	max_sub_val        int // one person_info record can have max value of sub.
	counter_insert     uint32
	counter_update     uint32
	counter_all_update uint32
	counter_delete     uint32
}

func Init() (*MyHandler, error) {
	myHandler := new(MyHandler)
	usr_name := os.Getenv("MYSQL_USR_NAME")
	if usr_name == "" {
		log.Fatal(errors.New("empty MYSQL_USR_NAME"))
	}
	usr_passwd := os.Getenv("MYSQL_USR_PASSWD")
	if usr_passwd == "" {
		log.Fatal(errors.New("empty MYSQL_USR_PASSWD"))
	}
	server_url := os.Getenv("MYSQL_SERVER_URL")
	if server_url == "" {
		log.Fatal(errors.New("empty MYSQL_SERVER_URL"))
	}
	server_port := os.Getenv("MYSQL_SERVER_PORT")
	if server_port == "" {
		server_port = "3306"
	}
	database_name := os.Getenv("MYSQL_DATABASE_NAME")
	if database_name == "" {
		database_name = "db_world"
	}
	mysql_endpoint := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", usr_name, usr_passwd, server_url, server_port, database_name)
	fmt.Println(mysql_endpoint + "?charset=utf8")
	db, err := sql.Open("mysql", mysql_endpoint + "?charset=utf8")
	if err != nil {
		fmt.Println("database initialize error : ", err.Error())
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	myHandler.db = db
	myHandler.max_sub_num = 100
	myHandler.max_sub_val = 500
	myHandler.counter_insert = 0
	myHandler.counter_update = 0
	myHandler.counter_all_update = 0
	myHandler.counter_delete = 0
	return myHandler, nil
}

func (myHandler *MyHandler) Close() {
	if myHandler.db != nil {
		myHandler.db.Close()
	}
}

func (myHandler *MyHandler) InsertPersonRecord() error {
	if myHandler.db == nil {
		return errors.New("database is nil, please initialize it")
	}
	sub_num := rand.Intn(myHandler.max_sub_num)
	if sub_num == 0 {
		sub_num = 1
	}
	sub_array := make([]int, sub_num)
	var array_sum int
	for i := 0; i < sub_num; i++ {
		sub_array[i] = rand.Intn(myHandler.max_sub_val)
		array_sum += sub_array[i]
	}
	var err error
	tx, err := myHandler.db.Begin()
	if err != nil {
		return err
	}
	result, err := tx.Exec("INSERT INTO persons (age, sum) VALUES (?, ?)", rand.Intn(100), array_sum)
	if err != nil {
		tx.Rollback()
		return err
	}
	person_id, _ := result.LastInsertId()
	for i := 0; i < sub_num; i++ {
		_, err = tx.Exec("INSERT INTO person_infos (person_id, sub, info) VALUES (?, ?, ?)", person_id, sub_array[i], "null")
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	atomic.AddUint32(&myHandler.counter_insert, 1)
	return nil
}

func (myHandler *MyHandler) UpdateAllPersonAge() error {
	if myHandler.db == nil {
		return errors.New("database is nil, please initialize it")
	}
	var err error
	tx, err := myHandler.db.Begin()
	if err != nil {
		tx.Rollback()
		return err
	}
	_, err = tx.Exec("update persons set age = age + 1")
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	atomic.AddUint32(&myHandler.counter_all_update, 1)
	return nil
}

func (myHandler *MyHandler) UpdatePersonRecord() error {
	if myHandler.db == nil {
		return errors.New("database is nil, please initialize it")
	}
	var err error
	tx, err := myHandler.db.Begin()
	if err != nil {
		tx.Rollback()
		return err
	}
	rows := tx.QueryRow("select count(*) as person_infos_count from person_infos")
	var person_infos_count int
	err = rows.Scan(&person_infos_count)
	if err != nil {
		tx.Rollback()
		return err
	}
	var af_rows int64 = 0
	for i := 0; i < rand.Intn(5)+1; i++ {
		limit_pos := rand.Intn(person_infos_count)
		rows = tx.QueryRow("select id as person_infos_id from person_infos limit ?, 1", limit_pos)
		var person_infos_id int
		err = rows.Scan(&person_infos_id)
		if err != nil {
			tx.Rollback()
			return err
		}
		change_val := rand.Intn(myHandler.max_sub_val) - (myHandler.max_sub_val / 2)
		if change_val == 0 {
			change_val = 1
		}
		res, err := tx.Exec("update person_infos as i join persons as p on i.person_id = p.id set i.sub = i.sub + ?, p.sum = p.sum + ? where i.id = ?", change_val, change_val, person_infos_id)
		if err != nil {
			tx.Rollback()
			return err
		}
		ar, err := res.RowsAffected()
		if err != nil {
			tx.Rollback()
			return err
		}
		af_rows += ar
	}
	if af_rows == 0 {
		tx.Rollback()
		return errors.New("update 0 rows")
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	atomic.AddUint32(&myHandler.counter_update, 1)
	return nil
}

func (myHandler *MyHandler) DeletePersonRecord() error {
	if myHandler.db == nil {
		return errors.New("database is nil, please initialize it")
	}
	tx, err := myHandler.db.Begin()
	if err != nil {
		tx.Rollback()
		return err
	}
	rows := tx.QueryRow("select count(*) as persons_count from persons")
	var persons_count int
	err = rows.Scan(&persons_count)
	if err != nil {
		tx.Rollback()
		return err
	}
	limit_pos := rand.Intn(persons_count)
	rows = tx.QueryRow("select id as persons_id from person_infos limit ?, 1", limit_pos)
	var persons_id int
	err = rows.Scan(&persons_id)
	if err != nil {
		tx.Rollback()
		return err
	}
	res, err := tx.Exec("delete from person_infos where person_id = ?", persons_id)
	if err != nil {
		tx.Rollback()
		return err
	}
	af_rows_, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		return err
	}
	if af_rows_ == 0 {
		tx.Rollback()
		return errors.New("delete op affects 0 rows")
	}
	res, err = tx.Exec("delete from persons where id = ?", persons_id)
	if err != nil {
		tx.Rollback()
		return err
	}
	af_rows_, err = res.RowsAffected()
	if err != nil {
		tx.Rollback()
		return err
	}
	if af_rows_ == 0 {
		tx.Rollback()
		return errors.New("delete op affects 0 rows")
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	atomic.AddUint32(&myHandler.counter_delete, 1)
	return nil
}

func (myHandler *MyHandler) TrigerAdHocSnapshot(id string) {
	tx, err := myHandler.db.Begin()
	if err != nil {
		fmt.Println("insert into debezium_signal error: ", err.Error())
	}
	res, err := tx.Exec("insert into db_world.debezium_signal values (?, 'execute-snapshot', '{\"data-collections\": [\"db_world.persons\",\"db_world.person_infos\"], \"type\": \"incremental\"}');", id)
	if err != nil {
		tx.Rollback()
		fmt.Println("insert into debezium_signal error: ", err.Error())
		return
	}
	ar, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		fmt.Println("insert into debezium_signal error: ", err.Error())
		return
	}
	if ar != 1 {
		tx.Rollback()
		fmt.Println("insert into debezium_signal error: affected rows is ", ar)
		return
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		fmt.Println("insert into debezium_signal error: ", err.Error())
		return
	}
	fmt.Printf("insert into debezium_signal with id %s\n", id)
}

func (myHandler *MyHandler) Report() {
	fmt.Printf("insert times: %v \t", myHandler.counter_insert)
	fmt.Printf("update times: %v \t", myHandler.counter_update)
	fmt.Printf("all update times: %v \t", myHandler.counter_all_update)
	fmt.Printf("delete times: %v \t", myHandler.counter_delete)
	fmt.Printf("total times: %v \n", myHandler.counter_insert+myHandler.counter_update+myHandler.counter_all_update+myHandler.counter_delete)
}

type CounterSummary struct {
	counter_insert     uint32
	counter_update     uint32
	counter_all_update uint32
	counter_delete     uint32
}

func (counterSummary *CounterSummary) Add(myHandler *MyHandler) {
	atomic.AddUint32(&counterSummary.counter_insert, myHandler.counter_insert)
	atomic.AddUint32(&counterSummary.counter_update, myHandler.counter_update)
	atomic.AddUint32(&counterSummary.counter_all_update, myHandler.counter_all_update)
	atomic.AddUint32(&counterSummary.counter_delete, myHandler.counter_delete)
}

func (counterSummary *CounterSummary) Report() {
	fmt.Printf("insert times: %v \t", counterSummary.counter_insert)
	fmt.Printf("update times: %v \t", counterSummary.counter_update)
	fmt.Printf("all update times: %v \t", counterSummary.counter_all_update)
	fmt.Printf("delete times: %v \t", counterSummary.counter_delete)
	fmt.Printf("total times: %v \n", counterSummary.counter_insert+counterSummary.counter_update+counterSummary.counter_all_update+counterSummary.counter_delete)
}

func main() {
	args := os.Args
	if len(args) != 3 {
		log.Fatal(errors.New("Error: You need input parallel number and simulate duration(second)"))
	}
	parallel_number, err := strconv.Atoi(args[1])
	if err != nil {
		fmt.Printf("your input %v can't be convert to int type.", args[1])
	}
	simulate_seconds, err := strconv.Atoi(args[2])
	if err != nil {
		fmt.Printf("your input %v can't be convert to int type.", args[1])
	}
	continueLabel := true
	var wg sync.WaitGroup
	counterSummary := CounterSummary{0, 0, 0, 0}
	fmt.Printf("\nsimulate parallel number: %v, duration: %vs, started at %v!\n", parallel_number, simulate_seconds, time.Now())
	for i := 0; i < parallel_number; i++ {
		wg.Add(1)
		go func(threadId int) {
			myHandler, _ := Init()
			errCount := make(map[string]int)
			for continueLabel {
				option := rand.Intn(3)
				var err error
				var msg string
				switch option {
				case 0:
					err = myHandler.InsertPersonRecord()
					msg = "insert"
				case 1:
					rd := rand.Intn(100)
					if rd < 20 {
						err = myHandler.UpdateAllPersonAge()
					} else {
						err = myHandler.UpdatePersonRecord()
					}
					msg = "update"
				case 2:
					err = myHandler.DeletePersonRecord()
					msg = "delete"
				}
				if err != nil {
					errCount[msg+": "+err.Error()]++
				}
				time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
			}
			for errType, count := range errCount {
				fmt.Println("thread_id:\t", threadId, "\t", errType, ":\t", count)
			}
			counterSummary.Add(myHandler)
			wg.Done()
		}(i)
	}
	var curr_ts = time.Now().Unix()
	snapshot_times := 4
	myHandler, _ := Init()
	for i := 0; i < snapshot_times; i++ {
		time.Sleep(time.Duration(simulate_seconds/snapshot_times) * time.Second)
		myHandler.TrigerAdHocSnapshot(fmt.Sprintf("go_%d_%d", curr_ts, i))
	}
	continueLabel = false
	wg.Wait()
	time.Sleep(time.Second * 3)
	myHandler.TrigerAdHocSnapshot(fmt.Sprintf("go_%d_lt", curr_ts))
	fmt.Printf("\nsummary \t")
	counterSummary.Report()
	fmt.Printf("\nsimulate finished at %v!\n", time.Now())
}