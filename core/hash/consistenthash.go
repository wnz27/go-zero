package hash

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/tal-tech/go-zero/core/lang"
	"github.com/tal-tech/go-zero/core/mapping"
)

const (
	// TopWeight is the top weight that one entry might set.
	TopWeight = 100

	minReplicas = 100
	prime       = 16777619
)

type (
	// Func defines the hash method.
	Func func(data []byte) uint64

	// A ConsistentHash is a ring hash implementation.
	ConsistentHash struct {
		hashFunc Func                            // 哈希函数
		replicas int                             // 虚拟节点放大因子
		keys     []uint64                        // 存储虚拟节点hash
		ring     map[uint64][]interface{}        // 虚拟节点与实际node的对应关系
		nodes    map[string]lang.PlaceholderType // 实际节点存储【便于快速查找，所以使用map】
		lock     sync.RWMutex                    // 锁
	}
)

// NewConsistentHash returns a ConsistentHash.
func NewConsistentHash() *ConsistentHash {
	return NewCustomConsistentHash(minReplicas, Hash)
}

// NewCustomConsistentHash returns a ConsistentHash with given replicas and hash func.
func NewCustomConsistentHash(replicas int, fn Func) *ConsistentHash {
	// 规范最小虚拟节点因子
	if replicas < minReplicas {
		replicas = minReplicas
	}

	// 手动可选参数可还行
	if fn == nil {
		fn = Hash
	}

	return &ConsistentHash{
		hashFunc: fn,
		replicas: replicas,
		ring:     make(map[uint64][]interface{}),
		nodes:    make(map[string]lang.PlaceholderType),
	}
}

// Add adds the node with the number of h.replicas,
// the later call will overwrite the replicas of the former calls.
func (h *ConsistentHash) Add(node interface{}) {
	h.AddWithReplicas(node, h.replicas)
}

// AddWithReplicas adds the node with the number of replicas,
// replicas will be truncated to h.replicas if it's larger than h.replicas,
// the later call will overwrite the replicas of the former calls.
func (h *ConsistentHash) AddWithReplicas(node interface{}, replicas int) {
	// 先删再加，使对象 h 的这个行为幂等
	h.Remove(node)

	if replicas > h.replicas {
		replicas = h.replicas
	}

	nodeRepr := repr(node)
	h.lock.Lock()
	defer h.lock.Unlock()
	h.addNode(nodeRepr)

	// 给真实节点增加replicas 数量的虚拟节点
	for i := 0; i < replicas; i++ {
		// 拿到虚拟节点
		hash := h.hashFunc([]byte(nodeRepr + strconv.Itoa(i)))
		h.keys = append(h.keys, hash)
		h.ring[hash] = append(h.ring[hash], node)
	}
	// 给虚拟节点排序
	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

// AddWithWeight adds the node with weight, the weight can be 1 to 100, indicates the percent,
// the later call will overwrite the replicas of the former calls.
func (h *ConsistentHash) AddWithWeight(node interface{}, weight int) {
	// don't need to make sure weight not larger than TopWeight,
	// because AddWithReplicas makes sure replicas cannot be larger than h.replicas
	replicas := h.replicas * weight / TopWeight
	h.AddWithReplicas(node, replicas)
}

// 测试下越界问题
func t_b_s(hash int) int {
	keys := []int{1, 2, 4, 5, 6, 10}
	res := sort.Search(len(keys), func(i int) bool {
		return keys[i] >= hash
	})
	return res
}

// Get returns the corresponding node from h base on the given v.
func (h *ConsistentHash) Get(v interface{}) (interface{}, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()

	if len(h.ring) == 0 {
		return nil, false
	}

	hash := h.hashFunc([]byte(repr(v)))
	// index = index of h.keys % len(h.keys) 加上求余防止越界, 你无法决定传来的v
	index := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	}) % len(h.keys)

	nodes := h.ring[h.keys[index]]
	switch len(nodes) {
	case 0:
		return nil, false
	case 1:
		return nodes[0], true
	default:
		innerIndex := h.hashFunc([]byte(innerRepr(v)))
		pos := int(innerIndex % uint64(len(nodes)))
		return nodes[pos], true
	}
}

// Remove removes the given node from h.
func (h *ConsistentHash) Remove(node interface{}) {
	nodeRepr := repr(node)
	// 整个移除操作加锁
	h.lock.Lock()
	defer h.lock.Unlock()
	// 如果没有这个
	if !h.containsNode(nodeRepr) {
		return
	}

	for i := 0; i < h.replicas; i++ {
		// 找虚拟节点
		hash := h.hashFunc([]byte(nodeRepr + strconv.Itoa(i)))

		// 二分找到虚拟节点索引
		index := sort.Search(len(h.keys), func(i int) bool {
			return h.keys[i] >= hash
		})

		// 如果这个虚拟节点有效，则把这个虚拟节点删掉
		if index < len(h.keys) && h.keys[index] == hash {
			h.keys = append(h.keys[:index], h.keys[index+1:]...)
		}
		h.removeRingNode(hash, nodeRepr)
	}

	h.removeNode(nodeRepr)
}

// delete real node from real nodes list of virtual hash node map
// 从虚拟节点hash对应的真实节点里删除 真实节点 nodeRepr
func (h *ConsistentHash) removeRingNode(hash uint64, nodeRepr string) {
	if nodes, ok := h.ring[hash]; ok {
		// 拿到虚拟节点对应的一堆真实节点
		newNodes := nodes[:0]     // 空list
		for _, x := range nodes { // 遍历真实节点 把不是nodeRepr的重新加进来
			if repr(x) != nodeRepr {
				newNodes = append(newNodes, x)
			}
		}
		// 如果 虚拟节点对应的 真实节点没有了，则把虚拟节点的对应的实际节点list从ring的map清除掉
		if len(newNodes) > 0 {
			h.ring[hash] = newNodes
		} else {
			delete(h.ring, hash)
		}
	}
}

// 增加真实节点
func (h *ConsistentHash) addNode(nodeRepr string) {
	h.nodes[nodeRepr] = lang.Placeholder
}

// 是否存在真实节点
func (h *ConsistentHash) containsNode(nodeRepr string) bool {
	_, ok := h.nodes[nodeRepr]
	return ok
}

// 删除真实节点，从nodes里
func (h *ConsistentHash) removeNode(nodeRepr string) {
	delete(h.nodes, nodeRepr)
}

func innerRepr(node interface{}) string {
	return fmt.Sprintf("%d:%v", prime, node)
}

// 返回 node 节点表示的 string 值, 如果 node 实现了 String() 方法则 直接会返回这个方法的值
func repr(node interface{}) string {
	return mapping.Repr(node)
}
