import pyarrow as pa

samp = """pa.schema([pa.field('activityrank', pa.int64(), True),
 pa.field('keep1', pa.int64(), True),
 pa.field('updatedby', pa.string(), True),
 pa.field('updated_utc_ts', pa.timestamp("ms"), True),
 pa.field('runid', pa.int64(), True)])"""

res = eval(samp)