package mr

import (
	"errors"
	"sync"
)

type listNode struct {
	data interface{}
	next *listNode
	prev *listNode
}

func (node *listNode) addBefore(data interface{}) {
	prev := node.prev

	insertNode := listNode{}
	insertNode.data = data

	insertNode.next = node
	node.prev = &insertNode
	insertNode.prev = prev
	prev.next = &insertNode
}

func (node *listNode) addAfter(data interface{}) {
	next := node.next

	insertNode := listNode{}
	insertNode.data = data

	insertNode.prev = node
	node.next = &insertNode
	insertNode.next = next
	next.prev = &insertNode
}

func (node *listNode) removeBefore() {
	prev := node.prev.prev
	node.prev = prev
	prev.next = node
}

func (node *listNode) removeAfter() {
	next := node.next.next
	node.next = next
	next.prev = node
}

type LinkedList struct {
	head  listNode
	count int
}

func (list *LinkedList) pushFront(data interface{}) {
	list.head.addAfter(data)
	list.count++
}

func (list *LinkedList) pushBack(data interface{}) {
	list.head.addBefore(data)
	list.count++
}

func (list *LinkedList) peekFront() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("peeking empty list")
	}
	return list.head.next, nil
}

func (list *LinkedList) peekBack() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("peeking empty list")
	}
	return list.head.prev.data, nil
}

func (list *LinkedList) popFront() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("pop empty list")
	}
	data := list.head.next.data
	list.head.removeAfter()
	list.count--
	return data, nil
}

func (list *LinkedList) popBack() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("pop empty list")
	}
	data := list.head.prev.data
	list.head.removeBefore()
	list.count--
	return data, nil
}

func (list *LinkedList) size() int {
	return list.count
}

func NewLinkedList() *LinkedList {
	list := LinkedList{}
	list.count = 0
	list.head.next = &list.head
	list.head.prev = &list.head

	return &list
}

type BlockQueue struct {
	list *LinkedList
	cond *sync.Cond
}

func NewBlockQueue() *BlockQueue {
	queue := BlockQueue{}

	queue.list = NewLinkedList()
	queue.cond = sync.NewCond(new(sync.Mutex))

	return &queue
}

func (queue *BlockQueue) putFront(data interface{}) {
	queue.cond.L.Lock()
	queue.list.pushFront(data)
	queue.cond.L.Unlock()
	queue.cond.Broadcast()
}

func (queue *BlockQueue) putBack(data interface{}) {
	queue.cond.L.Lock()
	queue.list.pushBack(data)
	queue.cond.L.Unlock()
	queue.cond.Broadcast()
}

func (queue *BlockQueue) getFront() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.count == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.popFront()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) getBack() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.count == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.popBack()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) popFront() (interface{}, error) {
	queue.cond.L.Lock()
	data, err := queue.list.popFront()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) popBack() (interface{}, error) {
	queue.cond.L.Lock()
	data, err := queue.list.popBack()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) size() int {
	queue.cond.L.Lock()
	ret := queue.list.count
	queue.cond.L.Unlock()
	return ret
}

func (queue *BlockQueue) peekFront() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.count == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.peekFront()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) peekBack() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.count == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.peekBack()
	queue.cond.L.Unlock()
	return data, err
}
