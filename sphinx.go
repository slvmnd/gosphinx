package gosphinx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

/* searchd command versions */
const(
	VER_MAJOR_PROTO			= 0x1
	VER_COMMAND_SEARCH		= 0x119
	VER_COMMAND_EXCERPT		= 0x103	// Latest version is 104. Change it to 104 if you use the Sphinx2.0.3 or higher version.
	VER_COMMAND_UPDATE		= 0x102
	VER_COMMAND_KEYWORDS	= 0x100
	VER_COMMAND_STATUS		= 0x100
	VER_COMMAND_FLUSHATTRS	= 0x100
)

/* matching modes */
const (
	SPH_MATCH_ALL = iota
	SPH_MATCH_ANY
	SPH_MATCH_PHRASE
	SPH_MATCH_BOOLEAN
	SPH_MATCH_EXTENDED
	SPH_MATCH_FULLSCAN
	SPH_MATCH_EXTENDED2
)

/* ranking modes (extended2 only) */
const(
	SPH_RANK_PROXIMITY_BM25 = iota //default mode, phrase proximity major factor and BM25 minor one
	SPH_RANK_BM25
	SPH_RANK_NONE
	SPH_RANK_WORDCOUNT
	SPH_RANK_PROXIMITY
	SPH_RANK_MATCHANY
	SPH_RANK_FIELDMASK
	SPH_RANK_SPH04
	SPH_RANK_TOTAL
)

/* sorting modes */
const(
	SPH_SORT_RELEVANCE = iota
	SPH_SORT_ATTR_DESC
	SPH_SORT_ATTR_ASC
	SPH_SORT_TIME_SEGMENTS
	SPH_SORT_EXTENDED
	SPH_SORT_EXPR
)

/* grouping functions */
const(
	SPH_GROUPBY_DAY   = iota
	SPH_GROUPBY_WEEK
	SPH_GROUPBY_MONTH
	SPH_GROUPBY_YEAR
	SPH_GROUPBY_ATTR
	SPH_GROUPBY_ATTRPAIR
)

/* searchd reply status codes */
const(
	SEARCHD_OK = iota
	SEARCHD_ERROR
	SEARCHD_RETRY
	SEARCHD_WARNING
)

/* attribute types */
const(
	SPH_ATTR_NONE		= iota
	SPH_ATTR_INTEGER
	SPH_ATTR_TIMESTAMP
	SPH_ATTR_ORDINAL
	SPH_ATTR_BOOL
	SPH_ATTR_FLOAT
	SPH_ATTR_BIGINT
	SPH_ATTR_STRING
	SPH_ATTR_MULTI		= 0x40000001
	SPH_ATTR_MULTI64	= 0x40000002
)

/* searchd commands */
const(
	SEARCHD_COMMAND_SEARCH = iota
	SEARCHD_COMMAND_EXCERPT
	SEARCHD_COMMAND_UPDATE
	SEARCHD_COMMAND_KEYWORDS
	SEARCHD_COMMAND_PERSIST
	SEARCHD_COMMAND_STATUS
	SEARCHD_COMMAND_QUERY
	SEARCHD_COMMAND_FLUSHATTRS
)

/* filter types */
const(
	SPH_FILTER_VALUES = iota
	SPH_FILTER_RANGE
	SPH_FILTER_FLOATRANGE
)

type filter struct {
	attr       string
	filterType int
	values     []uint64
	umin       uint64
	umax       uint64
	fmin       float32
	fmax       float32
	exclude    bool
}

type override struct {
	attrName   string
	attrType  int
	values map[uint64]interface{}
}

type SphinxMatch struct {
	DocId      uint64        // Matched document ID.
	Weight     int           // Matched document weight.
	AttrValues []interface{} // Matched document attribute values.
}

type SphinxWordInfo struct {
	Word string // Word form as returned from search daemon, stemmed or otherwise postprocessed.
	Docs int    // Total amount of matching documents in collection.
	Hits int    // Total amount of hits (occurences) in collection.
}

type SphinxResult struct {
	Fields     []string         // Full-text field namess.
	AttrNames  []string         // Attribute names.
	AttrTypes  []int            // Attribute types (refer to SPH_ATTR_xxx constants in SphinxClient).
	Matches    []SphinxMatch    // Retrieved matches.
	Total      int              // Total matches in this result set.
	TotalFound int              // Total matches found in the index(es).
	Time       float32          // Elapsed time (as reported by searchd), in seconds.
	Words      []SphinxWordInfo // Per-word statistics.

	Warning string
	Error   error
	Status  int // Query status (refer to SEARCHD_xxx constants in SphinxClient).
}

type SphinxClient struct {
	host string
	port int
	path string
	conn net.Conn

	offset        int    // how many records to seek from result-set start
	limit         int    // how many records to return from result-set starting at offset (default is 20)
	mode          int    // query matching mode (default is SPH_MATCH_ALL)
	weights       []int  // per-field weights (default is 1 for all fields)
	sort          int    // match sorting mode (default is SPH_SORT_RELEVANCE)
	sortBy        string // attribute to sort by (defualt is "")
	minId         uint64 // min ID to match (default is 0, which means no limit)
	maxId         uint64 // max ID to match (default is 0, which means no limit)
	filters       []filter
	groupBy       string // group-by attribute name
	groupFunc     int    // group-by function (to pre-process group-by attribute value with)
	groupSort     string // group-by sorting clause (to sort groups in result set with)
	groupDistinct string // group-by count-distinct attribute
	maxMatches    int    // max matches to retrieve
	cutoff        int    // cutoff to stop searching at
	retryCount    int
	retryDelay    int
	latitudeAttr  string
	longitudeAttr string
	latitude      float32
	longitude     float32

	warning   string
	connerror bool // connection error vs remote error flag
	timeout   int // millisecond, not nanosecond.

	reqs [][]byte // requests array for multi-query

	indexWeights map[string]int
	ranker       int	//排序模式
	maxQueryTime int
	fieldWeights map[string]int
	overrides    map[string]override
	selectStr      string //select-list (attributes or expressions, with optional aliases)
}

func NewSphinxClient() (sc *SphinxClient) {
	sc = new(SphinxClient)
	sc.host = "localhost"
	sc.port = 9312

	sc.limit = 20
	sc.mode = SPH_MATCH_ALL
	sc.sort = SPH_SORT_RELEVANCE
	sc.groupFunc = SPH_GROUPBY_DAY
	sc.groupSort = "@group desc"
	sc.maxMatches = 1000
	sc.timeout = 1000
	sc.ranker = SPH_RANK_PROXIMITY_BM25
	sc.selectStr = "*"

	return
}

/***** General API functions *****/

func (sc *SphinxClient) GetLastWarning() string {
	return sc.warning
}

func (sc *SphinxClient) SetServer(host string, port int) error {
	if host == "" {	return errors.New("SetServer -> host is empty!\n") }	
	sc.host = host
	if host[:1] == "/" {
		sc.path = host
	}
	if host[:7] == "unix://" {
		sc.path = host[7:]
	}
	
	if port <= 0 { return fmt.Errorf("SetServer -> port must be positive: %d\n", port) }
	sc.port = port
	return nil
}

func (sc *SphinxClient) SetRetries(count, delay int) error {
	if count < 0 { return fmt.Errorf("SetRetries -> count must not be negative: %d\n", count) }
	if delay < 0 { return fmt.Errorf("SetRetries -> delay must not be negative: %d\n", delay) }
	
	sc.retryCount = count
	sc.retryDelay = delay
	return nil
}

func (sc *SphinxClient) SetConnectTimeout(timeout int) error {
	if timeout < 0 { return fmt.Errorf("SetConnectTimeout -> connect timeout must not be negative: %d\n", timeout) }
	
	sc.timeout = timeout
	return nil
}

func (sc *SphinxClient) IsConnectError() bool {
	return sc.connerror
}

/***** General query settings *****/

// Set matches offset and limit to return to client, max matches to retrieve on server, and cutoff.
func (sc *SphinxClient) SetLimits(offset, limit, maxMatches, cutoff int) error {
	if offset < 0 { return fmt.Errorf("SetLimits -> offset must not be negative: %d\n", offset) }
	if limit <= 0 {	return fmt.Errorf("SetLimits -> limit must be positive: %d\n", limit) }
	if maxMatches <= 0 { return fmt.Errorf("SetLimits -> maxMatches must be positive: %d\n", maxMatches) }
	if cutoff < 0 { return fmt.Errorf("SetLimits -> cutoff must not be negative: %d\n", cutoff) }
	
	sc.offset = offset
	sc.limit = limit
	if maxMatches > 0 {
		sc.maxMatches = maxMatches
	}
	if cutoff > 0 {
		sc.cutoff = cutoff
	}
	return nil
}

// Set maximum query time, in milliseconds, per-index, 0 means "do not limit".
func (sc *SphinxClient) SetMaxQueryTime(maxQueryTime int) error {
	if maxQueryTime < 0 { return fmt.Errorf("SetMaxQueryTime -> maxQueryTime must not be negative: %d\n", maxQueryTime) }
	
	sc.maxQueryTime = maxQueryTime
	return nil
}

func (sc *SphinxClient) SetOverride(attrName string, attrType int, values map[uint64]interface{}) error {
	if attrName == "" { return errors.New("SetOverride -> attrName is empty!\n") }
	// Min value is 'SPH_ATTR_INTEGER = 1', not '0'.
	if (attrType < 1 || attrType > SPH_ATTR_STRING) && attrType != SPH_ATTR_MULTI && SPH_ATTR_MULTI != SPH_ATTR_MULTI64 {
		return fmt.Errorf("SetOverride -> invalid attrType: %d\n", attrType)
	}
	
	sc.overrides[attrName] = override{
		attrName : attrName,
		attrType : attrType,
		values : values,
	}
	return nil
}

func (sc *SphinxClient) SetSelect(s string) error {
	if s == "" { return errors.New("SetSelect -> selectStr is empty!\n") }

	sc.selectStr = s
	return nil
}

/***** Full-text search query settings *****/

func (sc *SphinxClient) SetMatchMode(mode int) error {
	if mode < 0 || mode > SPH_MATCH_EXTENDED2 {
		return fmt.Errorf("SetMatchMode -> unknown mode value; use one of the SPH_MATCH_xxx constants: %d\n", mode)
	}
	
	sc.mode = mode
	return nil
}

func (sc *SphinxClient) SetRankingMode(ranker int) error {
	// ranker >= SPH_RANK_TOTAL
	if ranker < 0 || ranker > SPH_RANK_TOTAL {
		return fmt.Errorf("SetRankingMode -> unknown ranker value; use one of the SPH_RANK_xxx constants: %d\n", ranker)
	}
	
	sc.ranker = ranker
	return nil
}

func (sc *SphinxClient) SetSortMode(mode int, sortBy string) error {
	if mode < 0 || mode > SPH_SORT_EXPR {
		return fmt.Errorf("SetSortMode -> unknown mode value; use one of the available SPH_SORT_xxx constants: %d\n", mode)
	}
	/*SPH_SORT_RELEVANCE ignores any additional parameters and always sorts matches by relevance rank.
	All other modes require an additional sorting clause.*/
	if (mode != SPH_SORT_RELEVANCE) && (sortBy == "") {
		return fmt.Errorf("SetSortMode -> sortby string must not be empty in selected mode: %d\n", mode)
	}
	
	sc.mode = mode
	sc.sortBy = sortBy
	return nil
}

func (sc *SphinxClient) SetFieldWeights(weights map[string]int) error {
	// Default weight value is 1.
	for field, weight := range weights {
		if weight < 1 {
			return fmt.Errorf("SetFieldWeights -> weights must be positive 32-bit integers, field:%s  weight:%d\n", field, weight)
		}
	}
	
	sc.fieldWeights = weights
	return nil
}

func (sc *SphinxClient) SetIndexWeights(weights map[string]int) error {
	for field, weight := range weights {
		if weight < 1 {
			return fmt.Errorf("SetIndexWeights -> weights must be positive 32-bit integers, field:%s  weight:%d\n", field, weight)
		}
	}

	sc.indexWeights = weights
	return nil
}

/***** Result set filtering settings *****/

func (sc *SphinxClient) SetIDRange(min, max uint64) error {
	if min > max {	return fmt.Errorf("SetIDRange -> min > max! min:%d  max:%d\n", min, max) }

	sc.minId = min
	sc.maxId = max
	return nil
}

func (sc *SphinxClient) SetFilter(attr string, values []uint64, exclude bool) error {
	if attr == "" { return fmt.Errorf("SetFilter -> attribute name is empty!\n") }
	if len(values) == 0 { return fmt.Errorf("SetFilter -> values is empty!\n") }
	
	sc.filters = append(sc.filters, filter{
		filterType : SPH_FILTER_VALUES,
		attr : attr,
		values : values,
		exclude : exclude,
	})
	return nil
}

func (sc *SphinxClient) SetFilterRange(attr string, umin, umax uint64, exclude bool) error {
	if attr == "" { return fmt.Errorf("SetFilterRange -> attribute name is empty!\n") }
	if umin > umax { return fmt.Errorf("SetFilterRange -> min > max! umin:%d  umax:%d\n", umin, umax) }
	
	sc.filters = append(sc.filters, filter{
		filterType : SPH_FILTER_RANGE,
		attr : attr,
		umin : umin,
		umax : umax,
		exclude : exclude,
	})
	return nil
}

func (sc *SphinxClient) SetFilterFloatRange(attr string, fmin, fmax float32, exclude bool) error {
	if attr == "" { return fmt.Errorf("SetFilterFloatRange -> attribute name is empty!\n") }
	if fmin > fmax { return fmt.Errorf("SetFilterFloatRange -> min > max! fmin:%d  fmax:%d\n", fmin, fmax) }
	
	sc.filters = append(sc.filters, filter{
		filterType : SPH_FILTER_FLOATRANGE,
		attr : attr,
		fmin : fmin,
		fmax : fmax,
		exclude : exclude,
	})
	return nil
}

func (sc *SphinxClient) SetGeoAnchor(latitudeAttr, longitudeAttr string, latitude, longitude float32) error {
	if latitudeAttr == "" { return fmt.Errorf("SetGeoAnchor -> latitudeAttr is empty!\n") }
	if longitudeAttr == "" { return fmt.Errorf("SetGeoAnchor -> longitudeAttr is empty!\n") }
	
	sc.latitudeAttr = latitudeAttr
	sc.longitudeAttr = longitudeAttr
	sc.latitude = latitude
	sc.longitude = longitude
	return nil
}

/***** GROUP BY settings *****/

func (sc *SphinxClient) SetGroupBy(groupBy string, groupFunc int, groupSort string) error {
	if groupFunc < 0 || groupFunc > SPH_GROUPBY_ATTRPAIR {
		return fmt.Errorf("SetGroupBy -> unknown groupFunc value: '%d', use one of the available SPH_GROUPBY_xxx constants.\n", groupFunc)
	}
	
	sc.groupBy = groupBy
	sc.groupFunc = groupFunc
	sc.groupSort = groupSort
	return nil
}

func (sc *SphinxClient) SetGroupDistinct(groupDistinct string){
	sc.groupDistinct = groupDistinct
}

/***** Querying *****/

func (sc *SphinxClient) Query(query, index, comment string) (result *SphinxResult, err error) {
	if index == "" { index = "*" }
	
	// reset requests array
	sc.reqs = nil

	sc.AddQuery(query, index, comment)
	results, err := sc.RunQueries()
	if err != nil {
		return nil, err
	}

	result = &results[0]
	if result.Error != nil {
		return nil, result.Error
	}
	
	sc.warning = result.Warning
	return
}

func (sc *SphinxClient) AddQuery(query, index, comment string) (i int, err error) {
	var req []byte

	req = writeInt32ToBytes(req, sc.offset)
	req = writeInt32ToBytes(req, sc.limit)
	req = writeInt32ToBytes(req, sc.mode)
	req = writeInt32ToBytes(req, sc.ranker)
	req = writeInt32ToBytes(req, sc.sort)
	req = writeLenStrToBytes(req, sc.sortBy)
	req = writeLenStrToBytes(req, query)

	req = writeInt32ToBytes(req, len(sc.weights))
	for _, w := range sc.weights {
		req = writeInt32ToBytes(req, w)
	}

	req = writeLenStrToBytes(req, index)

	req = writeInt32ToBytes(req, 1) // id64 range marker
	req = writeInt64ToBytes(req, sc.minId)
	req = writeInt64ToBytes(req, sc.maxId)

	req = writeInt32ToBytes(req, len(sc.filters))

	for _, f := range sc.filters {
		req = writeLenStrToBytes(req, f.attr)
		req = writeInt32ToBytes(req, f.filterType)
		
		switch f.filterType {
		case SPH_FILTER_VALUES:
			req = writeInt32ToBytes(req, len(f.values))
			for _, v := range f.values {
				req = writeInt64ToBytes(req, v)
			}
		case SPH_FILTER_RANGE:
			req = writeInt64ToBytes(req, f.umin)
			req = writeInt64ToBytes(req, f.umax)
		case SPH_FILTER_FLOATRANGE:
			req = writeFloat32ToBytes(req, f.fmin)
			req = writeFloat32ToBytes(req, f.fmax)
		}
		
		if f.exclude {
			req = writeInt32ToBytes(req, 1)
		} else {
			req = writeInt32ToBytes(req, 0)
		}
	}

	req = writeInt32ToBytes(req, sc.groupFunc)
	req = writeLenStrToBytes(req, sc.groupBy)

	req = writeInt32ToBytes(req, sc.maxMatches)
	req = writeLenStrToBytes(req, sc.groupSort)

	req = writeInt32ToBytes(req, sc.cutoff)
	req = writeInt32ToBytes(req, sc.retryCount)
	req = writeInt32ToBytes(req, sc.retryDelay)

	req = writeLenStrToBytes(req, sc.groupDistinct)

	if sc.latitudeAttr == "" || sc.longitudeAttr == "" {
		req = writeInt32ToBytes(req, 0)
	} else {
		req = writeInt32ToBytes(req, 1)
		req = writeLenStrToBytes(req, sc.latitudeAttr)
		req = writeLenStrToBytes(req, sc.longitudeAttr)
		req = writeFloat32ToBytes(req, sc.latitude)
		req = writeFloat32ToBytes(req, sc.longitude)
	}

	req = writeInt32ToBytes(req, len(sc.indexWeights))
	for n, v := range sc.indexWeights {
		req = writeLenStrToBytes(req, n)
		req = writeInt32ToBytes(req, v)
	}

	req = writeInt32ToBytes(req, sc.maxQueryTime)

	req = writeInt32ToBytes(req, len(sc.fieldWeights))
	for n, v := range sc.fieldWeights {
		req = writeLenStrToBytes(req, n)
		req = writeInt32ToBytes(req, v)
	}

	req = writeLenStrToBytes(req, comment)

	// attribute overrides
	req = writeInt32ToBytes(req, len(sc.overrides))
	for _, override := range sc.overrides {
		req = writeLenStrToBytes(req, override.attrName)
		req = writeInt32ToBytes(req, override.attrType)
		req = writeInt32ToBytes(req, len(override.values))
		for id, v := range override.values {
			req = writeInt64ToBytes(req, id)
			switch override.attrType {
			case SPH_ATTR_INTEGER:
				req = writeInt32ToBytes(req, v.(int))
			case SPH_ATTR_FLOAT:
				req = writeFloat32ToBytes(req, v.(float32))
			case SPH_ATTR_BIGINT:
				req = writeInt64ToBytes(req, v.(uint64))
			default:
				return -1, fmt.Errorf("AddQuery -> attr value is not int/float32/uint64.\n")
			}
		}
	}

	// select-list
	req = writeLenStrToBytes(req, sc.selectStr)

	// send query, get response
	sc.reqs = append(sc.reqs, req)
	return len(sc.reqs) - 1, nil
}

//Returns None on network IO failure; or an array of result set hashes on success.
func (sc *SphinxClient) RunQueries() (results []SphinxResult, err error) {
	if len(sc.reqs) == 0 {
		return nil, fmt.Errorf("RunQueries -> No queries defined, issue AddQuery() first.\n")
	}

	nreqs := len(sc.reqs)
	var allReqs []byte
	
	allReqs = writeInt32ToBytes(allReqs, 0)	// it's a client
	allReqs = writeInt32ToBytes(allReqs, nreqs)
	for _, req := range sc.reqs {
		allReqs = append(allReqs, req...)
	}

	response, err := sc.doRequest(SEARCHD_COMMAND_SEARCH, VER_COMMAND_SEARCH, allReqs)
	if err != nil {
		return nil, err
	}

	p := 0
	for i := 0; i < nreqs; i++ {
		var result = SphinxResult{Status: -1} // Default value of stauts is 0, but SEARCHD_OK = 0, so must set it to another num.
		result.Status = int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		if result.Status != SEARCHD_OK {
			length := int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
			message := response[p : p+length]
			p += length

			if result.Status == SEARCHD_WARNING {
				result.Warning = string(message)
			} else {
				result.Error = errors.New(string(message))
				continue
			}
		}

		// read schema
		nfields := int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		result.Fields = make([]string, nfields)
		for fieldNum := 0; fieldNum < nfields; fieldNum++ {
			fieldLen := int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
			result.Fields[fieldNum] = string(response[p : p+fieldLen])
			p += fieldLen
		}

		nattrs := int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		result.AttrNames = make([]string, nattrs)
		result.AttrTypes = make([]int, nattrs)
		for attrNum := 0; attrNum < nattrs; attrNum++ {
			attrLen := int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
			result.AttrNames[attrNum] = string(response[p : p+attrLen])
			p += attrLen
			result.AttrTypes[attrNum] = int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
		}

		// read match count
		count := int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		id64 := binary.BigEndian.Uint32(response[p : p+4]) // if id64 == 1, then docId is uint64
		p += 4
		result.Matches = make([]SphinxMatch, count)
		for matchesNum := 0; matchesNum < count; matchesNum++ {
			var match SphinxMatch
			if id64 == 1 {
				match.DocId = binary.BigEndian.Uint64(response[p : p+8])
				p += 8
			} else {
				match.DocId = uint64(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4
			}
			match.Weight = int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4

			match.AttrValues = make([]interface{}, nattrs)

			for attrNum := 0; attrNum < len(result.AttrTypes); attrNum++ {
				attrType := result.AttrTypes[attrNum]
				switch attrType {
				case SPH_ATTR_BIGINT:
					match.AttrValues[attrNum] = binary.BigEndian.Uint64(response[p : p+8])
					p += 8
				case SPH_ATTR_FLOAT:
					var f float32
					buf := bytes.NewBuffer(response[p : p+4])
					if err := binary.Read(buf, binary.BigEndian, &f); err != nil {
						return nil, err
					}
					match.AttrValues[attrNum] = f
					p += 4
				case SPH_ATTR_STRING:
					slen := int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4
					match.AttrValues[attrNum] = ""
					if slen > 0 {
						match.AttrValues[attrNum] = response[p : p+slen]
					}
					p += slen //p += slen-4
				case SPH_ATTR_MULTI: // SPH_ATTR_MULTI is 2^30+1, not an int value.
					nvals := int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4
					var vals = make([]uint32, nvals)
					for valNum := 0; valNum < nvals; valNum++ {
						vals[valNum] = binary.BigEndian.Uint32(response[p : p+4])
						p += 4
					}
					match.AttrValues[attrNum] = vals
				case SPH_ATTR_MULTI64:
					nvals := int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4
					nvals = nvals/2
					var vals = make([]uint64, nvals)
					for valNum := 0; valNum < nvals; valNum++ {
						vals[valNum] = binary.BigEndian.Uint64(response[p : p+4])
						p += 8
					}
					match.AttrValues[attrNum] = vals
				default: // handle everything else as unsigned ints
					match.AttrValues[attrNum] = binary.BigEndian.Uint32(response[p : p+4])
					p += 4
				}
			}
			result.Matches[matchesNum] = match
		}

		result.Total = int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		result.TotalFound = int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4

		msecs := binary.BigEndian.Uint32(response[p : p+4])
		p += 4
		result.Time = float32(msecs / 1000.0)

		nwords := int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		
		result.Words = make([]SphinxWordInfo, nwords)
		for wordNum := 0; wordNum < nwords; wordNum++ {
			wordLen := int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
			result.Words[wordNum].Word = string(response[p : p+wordLen])
			p += wordLen
			result.Words[wordNum].Docs = int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
			result.Words[wordNum].Hits = int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
		}
		results = append(results, result)
	}

	return results, nil
}

func (sc *SphinxClient) ResetFilters(){
	sc.filters = []filter{}
	
	/* reset GEO anchor */
	sc.latitudeAttr = "";
	sc.longitudeAttr = "";
	sc.latitude = 0;
	sc.longitude = 0;
}

func (sc *SphinxClient) ResetGroupBy(){
	sc.groupBy = "";
	sc.groupFunc = SPH_GROUPBY_DAY;
	sc.groupSort = "@group desc";
	sc.groupDistinct = "";
}

/***** Additional functionality *****/

// all bool values are default false.
type ExcerptsOpts struct {
	BeforeMatch string // default is "<b>".
	AfterMatch string // default is "</b>".
	ChunkSeparator string	// A string to insert between snippet chunks (passages). Default is " ... ".
	Limit int	// Maximum snippet size, in symbols (codepoints). default is 256.
	Around int // How much words to pick around each matching keywords block. default is 5.
	ExactPhrase bool // Whether to highlight exact query phrase matches only instead of individual keywords.
	SinglePassage bool // Whether to extract single best passage only.
	UseBoundaries bool // Whether to additionaly break passages by phrase boundary characters, as configured in index settings with phrase_boundary directive.
	WeightOrder bool // Whether to sort the extracted passages in order of relevance (decreasing weight), or in order of appearance in the document (increasing position). 
	QueryMode bool // Whether to handle $words as a query in extended syntax, or as a bag of words (default behavior). 
	ForceAllWords bool // Ignores the snippet length limit until it includes all the keywords.
	LimitPassages int // Limits the maximum number of passages that can be included into the snippet. default is 0 (no limit).
	LimitWords int // Limits the maximum number of keywords that can be included into the snippet. default is 0 (no limit).
	StartPassageId int // Specifies the starting value of %PASSAGE_ID% macro (that gets detected and expanded in BeforeMatch, AfterMatch strings). default is 1.
	LoadFiles bool // Whether to handle $docs as data to extract snippets from (default behavior), or to treat it as file names, and load data from specified files on the server side. 
	LoadFilesScattered bool // It assumes "load_files" option, and works only with distributed snippets generation with remote agents. The source files for snippets could be distributed among different agents, and the main daemon will merge together all non-erroneous results. So, if one agent of the distributed index has 'file1.txt', another has 'file2.txt' and you call for the snippets with both these files, the sphinx will merge results from the agents together, so you will get the snippets from both 'file1.txt' and 'file2.txt'.
	HtmlStripMode string // HTML stripping mode setting. Defaults to "index", allowed values are "none", "strip", "index", and "retain".
	AllowEmpty bool // Allows empty string to be returned as highlighting result when a snippet could not be generated (no keywords match, or no passages fit the limit). By default, the beginning of original text would be returned instead of an empty string.
	PassageBoundary string // Ensures that passages do not cross a sentence, paragraph, or zone boundary (when used with an index that has the respective indexing settings enabled). String, allowed values are "sentence", "paragraph", and "zone".
	EmitZones bool // Emits an HTML tag with an enclosing zone name before each passage.
}

func (sc *SphinxClient) BuildExcerpts(docs []string, index, words string, opts ExcerptsOpts) (resDocs []string, err error) {
	if len(docs) == 0 { return nil, errors.New("BuildExcerpts -> Have no documents to process!\n") }
	if index == "" { return nil, errors.New("BuildExcerpts -> index name is empty!\n") }
	if words == "" { return nil, errors.New("BuildExcerpts -> Have no words to highlight!\n") }
	if opts.PassageBoundary != "" && opts.PassageBoundary != "sentence" && opts.PassageBoundary != "paragraph" && opts.PassageBoundary != "zone" {
		return nil, fmt.Errorf("BuildExcerpts -> PassageBoundary allowed values are 'sentence', 'paragraph', and 'zone', now is: %s\n", opts.PassageBoundary)
	}
	
	// Default values, all bool values are default false.
	if opts.BeforeMatch == "" { opts.BeforeMatch = "<b>" }
	if opts.AfterMatch == "" { opts.AfterMatch = "</b>" }
	if opts.ChunkSeparator == "" { opts.ChunkSeparator = "..." }
	if opts.HtmlStripMode == "" { opts.HtmlStripMode = "index" }
	if opts.Limit == 0 { opts.Limit = 256 }
	if opts.Around == 0 { opts.Around = 5 }
	if opts.StartPassageId == 0 { opts.StartPassageId = 1 }

	var req []byte
	req = writeInt32ToBytes(req, 0)
	
	iFlags := 1		// remove_spaces
	if opts.ExactPhrase != false {	iFlags |= 2 }
	if opts.SinglePassage != false { iFlags |= 4 }
	if opts.UseBoundaries != false { iFlags |= 8 }
	if opts.WeightOrder != false { iFlags |= 16 }
	if opts.QueryMode != false { iFlags |= 32 }
	if opts.ForceAllWords != false { iFlags |= 64 }
	if opts.LoadFiles != false { iFlags |= 128 }
	if opts.AllowEmpty != false { iFlags |= 256 }
	if opts.EmitZones != false { iFlags |= 256 }
	req = writeInt32ToBytes(req, iFlags)
	
	req = writeLenStrToBytes(req, index)
	req = writeLenStrToBytes(req, words)
	
	req = writeLenStrToBytes(req, opts.BeforeMatch)
	req = writeLenStrToBytes(req, opts.AfterMatch)
	req = writeLenStrToBytes(req, opts.ChunkSeparator)
	req = writeInt32ToBytes(req, opts.Limit)
	req = writeInt32ToBytes(req, opts.Around)
	req = writeInt32ToBytes(req, opts.LimitPassages)
	req = writeInt32ToBytes(req, opts.LimitWords)
	req = writeInt32ToBytes(req, opts.StartPassageId)
	req = writeLenStrToBytes(req, opts.HtmlStripMode)
	req = writeLenStrToBytes(req, opts.PassageBoundary)
	
	req = writeInt32ToBytes(req, len(docs))
	for _, doc := range docs {
		req = writeLenStrToBytes(req, doc)
	}
	
	response, err := sc.doRequest(SEARCHD_COMMAND_EXCERPT, VER_COMMAND_EXCERPT, req)
	if err != nil {
		return nil, err
	}
	
	resDocs = make([]string, len(docs))
	p := 0
	for i := 0; i < len(docs); i++ {
		length := int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		resDocs[i] = string(response[p : p+length])
		p += length
	}
	
	return resDocs, nil
}

/*
 Connect to searchd server and update given attributes on given documents in given indexes.
 values[*][0] is docId, must be an uint64.
 values[*][1:] should be int or []int(mva mode)
 'ndocs'	-1 on failure, amount of actually found and updated documents (might be 0) on success
*/
func (sc *SphinxClient) UpdateAttributes(index string, attrs []string, values [][]interface{}) (ndocs int, err error) {
	if index == "" { return -1, errors.New("UpdateAttributes -> index name is empty!\n") }
	if len(attrs) == 0 { return -1, errors.New("UpdateAttributes -> no attribute names provided!\n") }
	if len(values) == 0 { return -1, errors.New("UpdateAttributes -> no update entries provided!\n") }
	
	for _, v := range values {
		// values[*][0] is docId, so +1
		if len(v) != len(attrs) + 1 {
			return -1, fmt.Errorf("UpdateAttributes -> update entry has wrong length: %#v\n", v)
		}
	}
	
	var mva bool
	if _, ok := values[0][1].([]int); ok {
		mva = true
	}
	
	var req []byte
	req = writeLenStrToBytes(req, index)
	
	req = writeInt32ToBytes(req, len(attrs))
	
	for _, attr := range attrs {
		req = writeLenStrToBytes(req, attr)
		if mva {
			req = writeInt32ToBytes(req, 1)
		} else {
			req = writeInt32ToBytes(req, 0)
		}
	}
	
	req = writeInt32ToBytes(req, len(values))
	for i:=0; i<len(values); i++ {
		if docId, ok := values[i][0].(uint64); !ok {
			return -1, fmt.Errorf("UpdateAttributes -> docId must be uint64: %#v\n", docId)
		} else {
			req = writeInt64ToBytes(req, docId)
		}
		for j:=1; j<len(values[i]); j++ {
			if mva {
				vars, ok := values[i][j].([]int)
				if !ok {
					return -1, fmt.Errorf("UpdateAttributes -> must be []int in mva mode: %#v\n", vars)
				}
				req = writeInt32ToBytes(req, len(vars))
				for _, v := range vars {
					req = writeInt32ToBytes(req, v)
				}
			} else {
				v, ok := values[i][j].(int)
				if !ok {
					return -1, fmt.Errorf("UpdateAttributes -> must be int if not in mva mode: %#v\n", values[i][j])
				}
				req = writeInt32ToBytes(req, v)
			}
		}
	}
	
	response, err := sc.doRequest(SEARCHD_COMMAND_UPDATE, VER_COMMAND_UPDATE, req)
	if err != nil {
		return -1, err
	}
	
	ndocs = int(binary.BigEndian.Uint32(response[0:4]))
	return
}

type Keyword struct {
	Tokenized string
	Normalized string
	Docs int
	Hits int
}
// Connect to searchd server, and generate keyword list for a given query.
// Returns null on failure, an array of Maps with misc per-keyword info on success.
func (sc *SphinxClient) BuildKeywords(query,index string, hits bool) (keywords []Keyword, err error) {
	var req []byte
	req = writeLenStrToBytes(req, query)
	req = writeLenStrToBytes(req, index)
	if hits {
		req = writeInt32ToBytes(req, 1)
	} else {
		req = writeInt32ToBytes(req, 0)
	}
	
	response, err := sc.doRequest(SEARCHD_COMMAND_KEYWORDS, VER_COMMAND_KEYWORDS, req)
	if err != nil {
		return nil, err
	}
	
	p := 0
	nwords := int(binary.BigEndian.Uint32(response[p : p+4]))
	p += 4
	
	keywords = make([]Keyword, nwords)
	
	for i:=0; i < nwords; i++ {
		var k Keyword
		length := int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		k.Tokenized = string(response[p : p+length])
		p += length
		
		length = int(binary.BigEndian.Uint32(response[p : p+4]))
		p += 4
		k.Normalized = string(response[p : p+length])
		p += length
		
		if hits {
			k.Docs = int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
			k.Hits = int(binary.BigEndian.Uint32(response[p : p+4]))
			p += 4
		}
		keywords[i] = k
	}
	
	return
}

func EscapeString(s string) string {
	chars := []string{`\`, `(`, `)`, `|`, `-`, `!`, `@`, `~`, `"`, `&`, `/`, `^`, `$`, `=`}
	for _, char := range chars {
		s = strings.Replace(s, char, `\`+char, -1)
	}
	return s
}

func (sc *SphinxClient) Status() (response [][]string, err error) {
	var req []byte
	req = writeInt32ToBytes(req, 1)

	res, err := sc.doRequest(SEARCHD_COMMAND_STATUS, VER_COMMAND_STATUS, req)
	if err != nil {
		return nil, err
	}

	p := 0
	rows := binary.BigEndian.Uint32(res[p : p+4])
	p += 4
	cols := binary.BigEndian.Uint32(res[p : p+4])
	p += 4

	response = make([][]string, rows)
	for i := 0; i < int(rows); i++ {
		response[i] = make([]string, cols)
		for j := 0; j < int(cols); j++ {
			length := int(binary.BigEndian.Uint32(res[p : p+4]))
			p += 4
			response[i][j] = string(res[p : p+length])
			p += length
		}
	}
	return response, nil
}

func (sc *SphinxClient) FlushAttributes()(iFlushTag int, err error){
	res, err := sc.doRequest(SEARCHD_COMMAND_FLUSHATTRS, VER_COMMAND_FLUSHATTRS, []byte{})
	if err != nil {
		return -1, err
	}
	
	if len(res) != 4 {
		return -1, errors.New("FlushAttributes -> unexpected response length!\n")
	}
	
	iFlushTag = int(binary.BigEndian.Uint32(res[0:4]))
	return
}

/***** Persistent connections *****/

func (sc *SphinxClient) connect() (conn net.Conn, err error) {
	if sc.conn != nil {
		return sc.conn, nil
	}
	
	// set connerror to false.
	sc.connerror = false
	
	// Try unix socket first.
	if sc.path != "" {
		conn, err = net.Dial("unix", sc.path)
		if err != nil {
			sc.connerror = true
			return nil, err
		}
	} else if sc.port > 0 {	
		conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", sc.host, sc.port))
		if err != nil {
			sc.connerror = true
			return nil, err
		}
	}
	
	deadTime := time.Now().Add(time.Duration(sc.timeout) * time.Millisecond)
	if err = conn.SetDeadline(deadTime); err != nil {
		sc.connerror = true
		return nil, err
	}
	
	header := make([]byte, 4)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		sc.connerror = true
		return nil, err
	}
	
	version := binary.BigEndian.Uint32(header)
	if version < 1 {
		return nil, fmt.Errorf("connect -> expected searchd protocol version 1+, got version %d\n", version)
	}

	// send my version
	_, err = conn.Write(writeInt32ToBytes([]byte{}, VER_MAJOR_PROTO))
	if err != nil {
		sc.connerror = true
		return nil, err
	}

	return conn, nil
}

func (sc *SphinxClient) Open() (err error) {
	if sc.conn != nil {
		return errors.New("Open -> already connected!\n")
	}

	if sc.conn, err = sc.connect();	err != nil {
		return err
	}

	var req []byte
	req = writeInt16ToBytes(req, SEARCHD_COMMAND_PERSIST)
	req = writeInt16ToBytes(req, 0)	// command version
	req = writeInt32ToBytes(req, 4) // body length
	req = writeInt32ToBytes(req, 1) // body
	_, err = sc.conn.Write(req)
	if err != nil {
		sc.connerror = true
		return err
	}

	return nil
}

func (sc *SphinxClient) Close() error {
	if sc.conn == nil {
		return errors.New("Close -> not connected!\n")
	}

	if err := sc.conn.Close(); err != nil {
		return err
	}
	
	sc.conn = nil
	return nil
}

func (sc *SphinxClient) doRequest(command int, version int, req []byte) (res []byte, err error) {
	defer func() {
		if x := recover(); x != nil {
			res = nil
			err = fmt.Errorf("doRequest panic -> %#v\n", x)
		}
	}()

	conn, err := sc.connect()
	if err != nil {
		return nil, err
	}

	var cmdVerLen []byte
	cmdVerLen = writeInt16ToBytes(cmdVerLen, command)
	cmdVerLen = writeInt16ToBytes(cmdVerLen, version)
	cmdVerLen = writeInt32ToBytes(cmdVerLen, len(req))
	req = append(cmdVerLen, req...)
	_, err = conn.Write(req)
	if err != nil {
		sc.connerror = true
		return nil, err
	}

	header := make([]byte, 8)
	if i, err := io.ReadFull(conn, header); err != nil {
		sc.connerror = true
		return nil, fmt.Errorf("doRequest -> just read %d bytes into header!\n", i)
	}
	
	status := binary.BigEndian.Uint16(header[0:2])	
	ver := binary.BigEndian.Uint16(header[2:4])
	size := binary.BigEndian.Uint32(header[4:8])
	if size <= 0 || size > 10*1024*1024 {
		return nil, fmt.Errorf("doRequest -> invalid response packet size (len=%d).\n", size)
	}
	
	res = make([]byte, size)
	if i, err := io.ReadFull(conn, res); err != nil {
		sc.connerror = true
		return nil, fmt.Errorf("doRequest -> just read %d bytes into res (size=%d).\n", i, size)
	}
	
	switch status {
	case SEARCHD_OK:
		// do nothing
	case SEARCHD_WARNING:
		wlen := binary.BigEndian.Uint32(res[0:4])
		sc.warning = string(res[4:wlen])
		res = res[4+wlen:]
	case SEARCHD_ERROR, SEARCHD_RETRY:
		wlen := binary.BigEndian.Uint32(res[0:4])
		return nil, errors.New(string(res[4:wlen]))
	default:
		return nil, fmt.Errorf("doRequest -> unknown status code (status=%d), ver: %d\n", status, ver)
	}

	return res, nil
}

func writeFloat32ToBytes(bs []byte, f float32) []byte {
	var byte4 = make([]byte, 4)
	buf := bytes.NewBuffer(byte4)
	if err := binary.Write(buf, binary.BigEndian, f); err != nil {
		fmt.Println(err)
	}
	byte4 = buf.Bytes()
	return append(bs, byte4...)
}

func writeInt16ToBytes(bs []byte, i int) []byte {
	var byte2 = make([]byte, 2)
	binary.BigEndian.PutUint16(byte2, uint16(i))
	return append(bs, byte2...)
}

func writeInt32ToBytes(bs []byte, i int) []byte {
	var byte4 = make([]byte, 4)
	binary.BigEndian.PutUint32(byte4, uint32(i))
	return append(bs, byte4...)
}

func writeInt64ToBytes(bs []byte, ui uint64) []byte {
	var byte8 = make([]byte, 8)
	binary.BigEndian.PutUint64(byte8, ui)
	return append(bs, byte8...)
}

func writeLenStrToBytes(bs []byte, s string) []byte {
	var byte4 = make([]byte, 4)
	binary.BigEndian.PutUint32(byte4, uint32(len(s)))
	bs = append(bs, byte4...)
	return append(bs, []byte(s)...)
}
