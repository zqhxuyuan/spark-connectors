

Gen hfile whater you use hbase client api, or spark rdd's saveAsNewAPIHadoopFile

```
➜  tdapp tree embedded_hbase
embedded_hbase
├── MasterProcWALs
│   └── state-00000000000000000001.log
├── WALs
│   ├── 192.168.199.104,55498,1505399008470
│   │   └── 192.168.199.104%2C55498%2C1505399008470.default.1505399026944
│   ├── 192.168.199.104,55503,1505399014702
│   │   └── 192.168.199.104%2C55503%2C1505399014702.default.1505399026971
│   ├── 192.168.199.104,55506,1505399015195
│   │   ├── 192.168.199.104%2C55506%2C1505399015195..meta.1505399024817.meta
│   │   └── 192.168.199.104%2C55506%2C1505399015195.default.1505399022429
│   ├── 192.168.199.104,55509,1505399015705
│   │   └── 192.168.199.104%2C55509%2C1505399015705.default.1505399022429
│   ├── 192.168.199.104,55512,1505399021234
│   │   └── 192.168.199.104%2C55512%2C1505399021234.default.1505399022426
│   └── 192.168.199.104,55532,1505399031486
│       └── 192.168.199.104%2C55532%2C1505399031486.default.1505399036998
├── data
│   ├── default
│   │   └── user     ============> TableName
│   │       └── b1e21b9ba4a77696e77e4323b439e6b0  ======> ColumnFamilyName
│   │           ├── info
│   │           │   └── 63dcc2cedbe645398773b09f5c33bb01_SeqId_4_  ===> HFile
│   │           └── recovered.edits
│   │               └── 2.seqid
│   └── hbase
│       ├── meta
│       │   └── 1588230740
│       │       ├── info
│       │       └── recovered.edits
│       │           └── 3.seqid
│       └── namespace
│           └── be0a01be1dc8823610bf94e366d55f9a
│               ├── info
│               └── recovered.edits
│                   └── 2.seqid
├── hbase.id
├── hbase.version
└── oldWALs
```

use hfile folder, such as .../data/default/user/63dcc2cedbe645398773b09f5c33bb01_SeqId_4_ as bulk load path.
here default is namespace, user is tablename,
b1e21b9ba4a77696e77e4323b439e6b0 is colum family name,
63dcc2cedbe645398773b09f5c33bb01_SeqId_4_ is hfile(s).

then use LoadIncrementalHFiles.doBulkLoad()...