package cluster

import (
	"Redis_Go/config"
	"Redis_Go/interface/database"
	"Redis_Go/interface/resp"
	"Redis_Go/lib/consistent_hash"
	"Redis_Go/lib/logger"
	"Redis_Go/resp/reply"
	"hash/crc32"
	"sort"
	"strings"
)

type ClusterDatabase struct {
	self       string                   // self node id
	nodes      []string                 // cluster nodes
	peerPicker *consistent_hash.NodeMap // consistent hash ring
	db         database.Database        // database instance (only DB0 in cluster mode)
}

// NewClusterDatabase creates a new ClusterDatabase instance with given db
func NewClusterDatabase(db database.Database) *ClusterDatabase {
	cluster := &ClusterDatabase{
		self:       config.Properties.Self,
		db:         db,
		peerPicker: consistent_hash.NewNodeMap(nil),
	}

	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	nodes = append(nodes, config.Properties.Peers...)
	nodes = append(nodes, config.Properties.Self)
	// Sort nodes to ensure all nodes have the same hash ring
	sort.Strings(nodes)
	cluster.nodes = nodes
	cluster.peerPicker.AddNode(nodes...)

	logger.Infof("Cluster initialized: self=%s, nodes=%v, peers=%v",
		cluster.self, cluster.nodes, config.Properties.Peers)

	return cluster
}

// Exec executes a command on the cluster database
func (c *ClusterDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	if len(args) == 0 {
		return reply.GetStandardErrorReply("ERR empty command")
	}

	cmdName := strings.ToLower(string(args[0]))

	// Disable SELECT command in cluster mode
	if cmdName == "select" {
		return reply.GetStandardErrorReply("ERR SELECT is not allowed in cluster mode")
	}

	// Extract key from command
	key := c.extractKey(cmdName, args)
	if key == "" {
		// Commands without key (PING, INFO, etc.) execute locally
		return c.db.Exec(client, args)
	}

	// Use consistent hash to pick target node
	targetNode := c.peerPicker.PickNode(key)

	// Debug: show all nodes for this key
	slot := c.getSlot(key)
	logger.Infof("key=%s, slot=%d, self=%s, targetNode=%s, nodes=%v",
		key, slot, c.self, targetNode, c.nodes)

	if targetNode == c.self {
		// Local node handles the command
		return c.db.Exec(client, args)
	}

	// Remote node: return MOVED redirection
	slot = c.getSlot(key)
	logger.Infof("Redirecting %s to %s for key %s", cmdName, targetNode, key)
	return reply.MakeMovedReply(slot, targetNode)
}

// extractKey extracts the key from command arguments
func (c *ClusterDatabase) extractKey(cmdName string, args [][]byte) string {
	switch cmdName {
	case "get", "set", "setnx", "getset", "strlen", "append", "setex",
		"exists", "del", "type", "expire", "ttl", "persist", "flushdb":
		if len(args) > 1 {
			return string(args[1])
		}
	case "mget", "mset":
		if len(args) > 1 {
			return string(args[1]) // return first key for routing
		}
	case "rename", "renamenx":
		if len(args) > 2 {
			// Check if both keys are on the same node
			srcKey := string(args[1])
			dstKey := string(args[2])
			srcNode := c.peerPicker.PickNode(srcKey)
			dstNode := c.peerPicker.PickNode(dstKey)
			if srcNode != dstNode {
				return "" // cross-node, needs special handling
			}
			return srcKey
		}
	}
	return ""
}

// getSlot calculates the slot for a given key using CRC32
func (c *ClusterDatabase) getSlot(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key))) % 16384
}

// Close closes the cluster database
func (c *ClusterDatabase) Close() {
	c.db.Close()
}

// AfterClientClose is called after a client closes
func (c *ClusterDatabase) AfterClientClose(client resp.Connection) {
	c.db.AfterClientClose(client)
}
