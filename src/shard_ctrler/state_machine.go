package shard_ctrler

import "sort"

type CtrlerStateMachine struct {
	Configs []Config
}

func NewCtrlerStateMachine() *CtrlerStateMachine {
	cf := &CtrlerStateMachine{Configs: make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

func (csm *CtrlerStateMachine) Query(num int) (Config, Err) {
	if len(csm.Configs) == 0 {
		return Config{}, ErrNoKey
	}

	if num < 0 || num >= len(csm.Configs) {
		return csm.Configs[len(csm.Configs)-1], OK
	}

	return csm.Configs[num], OK
}

func (csm *CtrlerStateMachine) Join(group map[int][]string) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// 将新的groups加入配置
	for gid, servers := range group {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	// 构造 gid -> shardid 映射关系
	// shard   gid
	//	 0      1
	//	 1      1
	//	 2      2
	//	 3      2
	//	 4      1
	// 转换后：
	//  gid       shard
	//   1       [0, 1, 4]
	//   2       [2, 3]
	gidToShards := map[int][]int{}
	for gid, _ := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// 进行 shard 迁移
	//   gid         shard
	//    1	     [0, 1, 4, 6]
	//    2	     [2, 3]
	//    3	     [5, 7]
	//    4	     []
	// ------------ 第1次移动 --------------
	//    1	     [1, 4, 6]
	//    2	     [2, 3]
	//    3	     [5, 7]
	//    4	     [0]
	// ------------ 第2次移动 --------------
	//    1	     [4, 6]
	//    2	     [2, 3]
	//    3	     [5, 7]
	//    4	     [0, 1]
	for {
		maxGid, minGid := gidWithMaxShards(gidToShards), gidWithMinShards(gidToShards)
		if maxGid != 0 && len(gidToShards[maxGid])-len(gidToShards[minGid]) <= 1 {
			break
		}

		// 最少shard的 gid 增加一个 shard
		gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
		// 最多shard的 gid 减少一个 shard
		gidToShards[maxGid] = gidToShards[maxGid][1:]
	}

	// 得到新的gid -> shard 信息之后，存储到 shards 数组中
	var newShards [NShards]int
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards

	csm.Configs = append(csm.Configs, newConfig)

	return OK
}

func (csm *CtrlerStateMachine) Leave(gids []int) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	// 构建新的配置
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// 构造 gid -> shard 的映射关系
	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// 删除对应的 gid，并且将对应的 shard 暂存起来
	var unassignedShards []int
	for _, gid := range gids {
		//	如果 gid 在 Group 中，则删除掉
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		// 取出对应的 shard
		if shards, ok := gidToShards[gid]; ok {
			unassignedShards = append(unassignedShards, shards...)
			delete(gidToShards, gid)
		}
	}

	var newShards [NShards]int
	// 重新分配被删除的 gid 对应的 shard
	if len(newConfig.Groups) != 0 {
		for _, shard := range unassignedShards {
			minGid := gidWithMinShards(gidToShards)
			gidToShards[minGid] = append(gidToShards[minGid], shard)
		}

		// 重新存储 shards 数组
		for gid, shards := range gidToShards {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}

	// 将配置保存
	newConfig.Shards = newShards
	csm.Configs = append(csm.Configs, newConfig)
	return OK
}

// todo: Move后不平衡怎么办
func (csm *CtrlerStateMachine) Move(shardid, gid int) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	// 构建新的配置
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	newConfig.Shards[shardid] = gid
	csm.Configs = append(csm.Configs, newConfig)
	return OK
}

func gidWithMaxShards(gidToShards map[int][]int) int {
	// todo: 这行代码意义是什么
	if shard, ok := gidToShards[0]; ok && len(shard) > 0 {
		return 0
	}

	// 为了让每个节点在调用的时候获取到的配置是一样的
	//	这里将 gid 进行排序，确保遍历的顺序是确定的
	gids := make([]int, 0, len(gidToShards))
	for gid, _ := range gidToShards {
		gids = append(gids, gid)
	}
	// 排序
	sort.Ints(gids)

	maxShards, maxGid := -1, -1
	for _, gid := range gids {
		if len(gidToShards[gid]) > maxShards {
			maxShards = len(gidToShards[gid])
			maxGid = gid
		}
	}

	return maxGid
}

func gidWithMinShards(gidToShars map[int][]int) int {
	var gids []int
	for gid := range gidToShars {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	minGid, minShards := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(gidToShars[gid]) < minShards {
			minGid, minShards = gid, len(gidToShars[gid])
		}
	}
	return minGid
}

func copyGroups(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}

	return newGroups
}
