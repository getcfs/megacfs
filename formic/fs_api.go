package formic

// Structures used in Group Store
//  File System
//  /acct/(uuid)/fs  "(uuid)"    { "id": "uuid", "name": "name", "status": "active",
//                                "createdate": <timestamp>, "deletedate": <timestamp>
//                               }
//
// IP Address
// /acct/(uuid)/fs/(uuid)/addr "(uuid)"   { "id": uuid, "addr": "111.111.111.111", "status": "active",
//                                         "createdate": <timestamp>, "deletedate": <timestamp>
//                                       }

type FileSysRef struct {
	FSID   string `json:"fsid"`
	AcctID string `json:"acctid"`
}

type FileSysAttr struct {
	Attr  string `json:"attr"`
	Value string `json:"value"`
	FSID  string `json:"fsid"`
}

type AddrRef struct {
	Addr string `json:"addr"`
	FSID string `json:"fsid"`
}

type FileSysMeta struct {
	ID     string   `json:"id"`
	AcctID string   `json:"acctid"`
	Name   string   `json:"name"`
	Status string   `json:"status"`
	Addr   []string `json:"addrs"`
}
