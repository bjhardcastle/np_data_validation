import data_validation as dv 
import timing

f = R"\\allen\programs\mindscope\workgroups\np-exp\1131646156_569154_20210930\1131646156_569154_20210930_probeABC\recording_slot2_10.npx2"
large_file = dv.CRC32DataValidationFile(path=f)

