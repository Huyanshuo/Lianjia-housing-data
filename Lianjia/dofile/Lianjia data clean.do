clear all

gl root_dir = "/Users/huyh/Desktop/Lianjia data/data_clean"
gl data_dir = "/Users/huyh/Desktop/Lianjia data/data_raw"
gl finaldata_dir = "/Users/huyh/Desktop/Lianjia data/finaldata"
gl dofile_dir = "/Users/huyh/Desktop/Lianjia data/dofile"
gl result_dir = "/Users/huyh/Desktop/Lianjia data/results"

cd "$root_dir"

import delimited "/Users/huyh/Desktop/Lianjia data/data_raw/Lianjia Housing Data/二手房挂牌数据2007-2023.csv", delimiter(",")

drop v1
drop v19

rename 城市 city
rename 行政区划代码 code
rename 所属省份 province
rename 所属地域 geoarea
rename 区域 district
rename 商圈 busdistrict
rename 小区 estate
rename 挂牌年份 year
rename 挂牌月份 month
rename 单价平均价格 price_sqm_avg
rename 单价中位数价格 price_sqm_med
rename 单价最低价格 price_sqm_min
rename 单价最高价格 price_sqm_max
rename 总价平均价格 price_ttl_avg
rename 总价中位数价格 price_ttl_med
rename 总价最低价格 price_ttl_min
rename 总价最高价格 price_ttl_max

drop if missing(code)
drop if missing(price_sqm_avg)

