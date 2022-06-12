package mr

type MapSet struct {
	mapBool map[interface{}]bool
	count   int
}

func newMapSet() *MapSet {
	mapSet := MapSet{}
	mapSet.mapBool = make(map[interface{}]bool)
	mapSet.count = 0
	return &mapSet
}

func (mapSet *MapSet) insert(data interface{}) {
	mapSet.mapBool[data] = true
	mapSet.count++
}

func (mapSet *MapSet) contains(data interface{}) bool {
	return mapSet.mapBool[data]
}

func (mapSet *MapSet) remove(data interface{}) {
	mapSet.mapBool[data] = false
	mapSet.count--
}

func (mapSet *MapSet) size() int {
	return mapSet.count
}
