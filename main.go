package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type op string

const (
	opAdd    op = "add"
	opUpdate op = "upd"
	opDelete op = "del"
)

const (
	maxUpdateTimes = 15

	clientNum = 5

	maxMainStep = 3 // 1-add 2-update 3-delete

	maxSleepInterval = 10

	maxDeleteBy = 2

	resultChanLen = 10

	testNum = 1000
)

var (
	errNameDup   = errors.New("nickname is duplicated")
	errMobileDup = errors.New("mobile is duplicated")

	opID int64
	uid  int64

	origin  *userSet
	replica *userSet
)

var (
	mobilePool = []string{
		"18702855916",
		"18702855917",
		"18702855918",
		"18702855919",
		"18702852016",
		"18702852116",
		"18702852211",
		"18702852312",
		"18702864914",
		"18702865910",
		"18702866919",
		"18702867913",
	}

	namePool = []string{
		"alice",
		"bob",
		"charles",
		"david",
		"eva",
		"fred",
		"green",
		"han",
		"ivan",
		"jacky",
		"linda",
		"mike",
	}

	agePool = []int{
		3,
		12,
		18,
		31,
		43,
		55,
		60,
		78,
	}
)

func nextOpID() int64 {
	return atomic.AddInt64(&opID, 1)
}

func nextUID() int64 {
	return atomic.AddInt64(&uid, 1)
}

type addLog struct {
	opNo int64
	op   op
	data userRecord
}

type updateLog struct {
	opNo    int64
	op      op
	newData userRecord
	oldData userRecord
}

type deleteLog struct {
	opNo int64
	op   op
	data userRecord
}

type opLog struct {
	op   op
	data interface{}
}

type userRecord struct {
	id       int64  //guid
	nickname string // local unique opID
	mobile   string // local unique opID
	age      int
}

type opLogWrapper struct {
	uid int64
	*opLog
}

func (ur *userRecord) match(rhs *userRecord) bool {
	if ur.id != rhs.id {
		return false
	}

	if ur.nickname != rhs.nickname {
		return false
	}

	if ur.mobile != rhs.mobile {
		return false
	}

	if ur.age != rhs.age {
		return false
	}

	return true
}

type userSet struct {
	mu      sync.Mutex
	users   map[int64]*userRecord
	names   map[string]*userRecord
	mobiles map[string]*userRecord
}

func newUserSet() *userSet {
	return &userSet{
		users:   make(map[int64]*userRecord),
		names:   make(map[string]*userRecord),
		mobiles: make(map[string]*userRecord),
	}
}

func (us *userSet) valid(r *userRecord) error {

	if _, ok := us.names[r.nickname]; ok {
		return errNameDup
	}

	if _, ok := us.mobiles[r.mobile]; ok {
		return errMobileDup
	}

	return nil

}
func (us *userSet) add(r *userRecord) (*addLog, error) {
	if r == nil {
		return nil, nil
	}
	us.mu.Lock()
	defer us.mu.Unlock()

	if err := us.valid(r); err != nil {
		return nil, err
	}

	us.addHelper(r)

	log := addLog{
		opNo: nextOpID(),
		op:   opAdd,
		data: *r,
	}

	return &log, nil
}

func (us *userSet) deleteHelper(r *userRecord) {
	delete(us.users, r.id)
	delete(us.names, r.nickname)
	delete(us.mobiles, r.mobile)
}

func (us *userSet) addHelper(r *userRecord) {
	us.users[r.id] = r
	us.mobiles[r.mobile] = r
	us.names[r.nickname] = r
}
func (us *userSet) update(id int64, r *userRecord) (*updateLog, error) {

	if r == nil {
		return nil, nil
	}

	us.mu.Lock()
	defer us.mu.Unlock()

	var (
		old *userRecord
		ok  bool
	)

	// not existed, no update
	if old, ok = us.users[id]; !ok {
		return nil, nil
	}

	// duplicated
	if err := us.valid(r); err != nil {
		return nil, err
	}

	us.deleteHelper(old)
	us.addHelper(r)

	updLog := updateLog{
		opNo:    nextOpID(),
		op:      opUpdate,
		newData: *r,
		oldData: *old,
	}

	return &updLog, nil
}

func (us *userSet) deleteByID(id int64) (*deleteLog, error) {

	us.mu.Lock()
	defer us.mu.Unlock()

	r, ok := us.users[id]
	if !ok {
		return nil, nil
	}

	us.deleteHelper(r)

	delLog := deleteLog{
		opNo: nextOpID(),
		op:   opDelete,
		data: *r,
	}
	return &delLog, nil
}

func (us *userSet) deleteByMobile(mobile string) (*deleteLog, error) {

	us.mu.Lock()
	defer us.mu.Unlock()

	r, ok := us.mobiles[mobile]
	if !ok {
		return nil, nil
	}

	us.deleteHelper(r)

	delLog := deleteLog{
		opNo: nextOpID(),
		op:   opDelete,
		data: *r,
	}
	return &delLog, nil
}

func (us *userSet) deleteByName(name string) (*deleteLog, error) {
	us.mu.Lock()
	defer us.mu.Unlock()

	r, ok := us.names[name]
	if !ok {
		return nil, nil
	}

	us.deleteHelper(r)

	delLog := deleteLog{
		opNo: nextOpID(),
		op:   opDelete,
		data: *r,
	}
	return &delLog, nil
}

func (us *userSet) match(rhs *userSet) bool {
	us.mu.Lock()
	defer us.mu.Unlock()

	for k, v := range us.users {
		v2, ok := rhs.users[k]
		if !ok {
			return false
		}

		if !v.match(v2) {
			return false
		}

	}
	return true
}

func (us *userSet) print(tag string) {

	if tag != "" {
		fmt.Println(tag)
	}
	us.mu.Lock()
	defer us.mu.Unlock()
	if len(us.users) == 0 {
		fmt.Println("the user set is empty now")
		return
	}

	for _, v := range us.users {
		fmt.Printf("\tuid: %d name: %s mobile: %s age: %d\n", v.id, v.nickname, v.mobile, v.age)
	}
	fmt.Println()
}

func sleep() {
	waitTime := time.Duration(rand.Int63()%maxSleepInterval) + 1
	time.Sleep(waitTime * time.Millisecond)
}

// add operation: uid=>-1
func doWithSleep(uid int64, us *userSet, op op, chOpLog chan *opLog) *userRecord {

	sleep()

	ur := userRecord{
		id:       uid,
		nickname: namePool[rand.Intn(len(namePool))],
		mobile:   mobilePool[rand.Intn(len(mobilePool))],
		age:      agePool[rand.Intn(len(agePool))],
	}

	var (
		data   interface{}
		err    error
		addLog *addLog
		updLog *updateLog
	)
	if op == opAdd {
		addLog, err = us.add(&ur)
		if addLog != nil {
			data = addLog
		}
	} else {
		updLog, err = us.update(uid, &ur)
		if updLog != nil {
			data = updLog
		}
	}

	if err != nil {
		// fmt.Println(err)
		return nil
	}

	// no such user
	if data == nil {
		return nil
	}

	chOpLog <- &opLog{
		op:   op,
		data: data,
	}
	return &ur

}

type peekChanOpLog struct {
	in <-chan *opLog
	v  *opLog
	ok bool
}

func newPeekChanOpLog(in <-chan *opLog) *peekChanOpLog {
	return &peekChanOpLog{in: in}
}
func (p *peekChanOpLog) peek() (*opLog, bool) {
	if !p.ok {
		p.v, p.ok = <-p.in
	}
	return p.v, p.ok
}

func (p *peekChanOpLog) get() (*opLog, bool) {
	if p.ok {
		p.ok = false
		return p.v, true
	}
	v, ok := <-p.in
	return v, ok
}

func transformUpdate(upd *updateLog) []opLogWrapper {

	delLog := opLogWrapper{
		uid: upd.newData.id,
		opLog: &opLog{
			op: opDelete,
			data: &deleteLog{
				opNo: upd.opNo,
				op:   opDelete,
				data: upd.oldData,
			},
		},
	}

	addLog := opLogWrapper{
		uid: upd.newData.id,
		opLog: &opLog{
			op: opAdd,
			data: &addLog{
				opNo: upd.opNo,
				op:   opAdd,
				data: upd.newData,
			},
		},
	}

	return []opLogWrapper{
		delLog,
		addLog,
	}

}

// filter filter out the delete logs
func filter(logs []opLogWrapper) map[int64]*opLog {

	// 保存每个uid在本段中的最后一个操作
	m := map[int64]*opLog{}

	var uid int64
	for i := len(logs) - 1; i >= 0; i-- {
		uid = logs[i].uid

		if _, ok := m[uid]; ok {
			continue
		}

		m[logs[i].uid] = logs[i].opLog

	}

	return m

}

func transform(logs []opLogWrapper, ch chan *opLog, wg *sync.WaitGroup) {

	defer wg.Done()

	var buf []opLogWrapper

	// update = delete + update
	for _, v := range logs {
		if v.op == opUpdate {
			ret := transformUpdate(v.data.(*updateLog))
			buf = append(buf, ret...)
			continue
		}

		buf = append(buf, v)
	}

	m := filter(buf)

	for _, v := range m {
		ch <- v
	}

}

func _map(chOpLog chan *opLog, chRet chan *opLog) {
	var (
		buf []opLogWrapper
		uid int64
	)

	ch := newPeekChanOpLog(chOpLog)

	var wg sync.WaitGroup
	for {
		curLog, ok := ch.get()
		if !ok {
			break
		}
		switch v := curLog.data.(type) {
		case *addLog:
			uid = v.data.id

		case *updateLog:
			uid = v.newData.id

		case *deleteLog:
			uid = v.data.id
		}

		buf = append(buf, opLogWrapper{
			uid:   uid,
			opLog: curLog,
		})

		if curLog.op == opUpdate || curLog.op == opDelete {
			nextLog, ok := ch.peek()
			if !ok {
				break
			}
			if nextLog.op == opAdd {
				wg.Add(1)
				transform(buf, chRet, &wg)
				buf = nil
			}
		}
	}

	wg.Add(1)
	if buf != nil {
		transform(buf, chRet, &wg)
		close(chRet)
	}

	wg.Wait()

}

func recreate(ch chan *opLog, us *userSet) {

	m := map[int64]*opLog{}

	var (
		uid     int64
		oldOpNo int64
		opNo    int64
	)
	for log := range ch {

		switch v := log.data.(type) {
		case *addLog:
			uid = v.data.id
			opNo = v.opNo

		case *deleteLog:
			uid = v.data.id
			opNo = v.opNo
		}

		if val, ok := m[uid]; !ok {
			m[uid] = log

		} else {

			switch t := val.data.(type) {
			case *addLog:
				oldOpNo = t.opNo

			case *deleteLog:
				oldOpNo = t.opNo

			}

			if oldOpNo < opNo {
				m[uid] = log
			}
		}

	}

	for k, v := range m {
		if v.op == opDelete {
			delete(m, k)
		}
	}

	for k, v := range m {
		if v.op == opDelete {
			delete(m, k)
		}

		ur := v.data.(*addLog)

		us.add(&ur.data)
	}

}
func recovery(chOpLog chan *opLog, us *userSet) {

	ch := make(chan *opLog, resultChanLen)
	_map(chOpLog, ch)

	recreate(ch, us)

}

func do(us *userSet, chOpLog chan *opLog) {

	// 1-add 2-update 3-delete
	// only one: add & delete, [0,maxUpdateTimes) update
	step := rand.Intn(maxMainStep) + 1

	const (
		add    = 1
		update = 2
		delete = 3
	)

	uid := nextUID()

	// add
	ur := doWithSleep(uid, us, opAdd, chOpLog)
	if step == add || ur == nil {
		return
	}

	// update
	updateTimes := rand.Intn(maxUpdateTimes)

	for i := 0; i < updateTimes; i++ {
		doWithSleep(uid, us, opUpdate, chOpLog)
	}

	if step == update {
		return
	}

	// 1-id 2-nickname 3-mobile
	// delete
	by := rand.Intn(maxDeleteBy)

	const (
		byID = iota
		byName
		byMobile
	)

	var (
		delLog *deleteLog
		err    error
	)
	switch by {
	case byID:
		delLog, err = us.deleteByID(ur.id)

	case byName:
		delLog, err = us.deleteByName(ur.nickname)

	case byMobile:
		delLog, err = us.deleteByMobile(ur.mobile)

	default:
		panic("never happen")

	}

	if err != nil {
		// fmt.Println(err)
		return
	}

	// no such user
	if delLog == nil {
		return
	}

	chOpLog <- &opLog{
		op:   opDelete,
		data: delLog,
	}

}

func match(origin, replica *userSet) bool {

	origin.print("origin")
	replica.print("replica")
	return origin.match(replica)
}

func main() {

	rand.Seed(time.Now().Unix())
	for i := 0; i < testNum; i++ {
		_main(i + 1)
	}
}
func _main(round int) {

	fmt.Printf("\nthe %d round is begining\n", round)
	defer fmt.Printf("the %d round is finished\n", round)

	chOpLog := make(chan *opLog, 10)
	chFinished := make(chan struct{}, clientNum)
	chDup := make(chan struct{}, 2)

	origin = newUserSet()
	replica = newUserSet()

	//us.print()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < clientNum; i++ {
			<-chFinished
		}

		close(chOpLog)
		chDup <- struct{}{}

	}()

	for i := 0; i < clientNum; i++ {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()
			do(origin, chOpLog)
			chFinished <- struct{}{}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		recovery(chOpLog, replica)
		chDup <- struct{}{}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-chDup
		<-chDup
		if ok := match(origin, replica); !ok {
			panic("recreate is failed")
		} else {
			fmt.Println("\trecreate is successfully")
		}
	}()

	wg.Wait()
	//us.print()

}
