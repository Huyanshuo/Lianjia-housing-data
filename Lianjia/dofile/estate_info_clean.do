clear all

gl root_dir = "/Users/huyh/Desktop/Lianjia data/data_clean"
gl data_dir = "/Users/huyh/Desktop/Lianjia data/data_raw"
gl finaldata_dir = "/Users/huyh/Desktop/Lianjia data/finaldata"
gl dofile_dir = "/Users/huyh/Desktop/Lianjia data/dofile"
gl result_dir = "/Users/huyh/Desktop/Lianjia data/results"

cd "$root_dir"

use "/Users/huyh/Desktop/Lianjia data/data_raw/Lianjia Housing Data/Clean_V1.dta"

duplicates drop estate, force

drop year month price_sqm_avg price_sqm_med price_sqm_min price_sqm_max price_ttl_avg price_ttl_med price_ttl_min price_ttl_max

gen full_location = city + "市" + district + cond(strpos(district, "区") == 0 & strpos(district, "市") == 0 & strpos(district, "县") == 0, "区", "") + estate

egen id_estate = seq()

save "estate_info.dta", replace
export delimited "estate_info.csv", replace
