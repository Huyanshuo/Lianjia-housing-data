clear all

gl root_dir = "C:\Users\lenovo\OneDrive - HKUST Connect\Desktop\UROP1100\Chengdu\data_clean"
gl data_dir = "C:\Users\lenovo\OneDrive - HKUST Connect\Desktop\UROP1100\Chengdu\data_raw"
gl finaldata_dir = "C:\Users\lenovo\OneDrive - HKUSTConnect\Desktop\UROP1100\Chengdu\finaldata"
gl dofile_dir = "C:\Users\lenovo\OneDrive - HKUST Connect\Desktop\UROP1100\Chengdu\dofile"
gl dofile_dir = "C:\Users\lenovo\OneDrive - HKUST Connect\Desktop\UROP1100\Chengdu\results"

cd "$root_dir"

import excel "$data_dir\Chengdu_transactions.xlsx " , firstrow


rename A ID
rename 小区名称 estate_name
rename 行政区域 district
rename 片区 jiedao
rename 建筑面积 size
rename 成交价 price_total
rename 单价 price_unit
rename 成交日期 date_transaction
drop if length(district) >10 | length(district) == 0
drop 挂牌价 成交周期 户型 成交渠道 朝向 装修 电梯 楼层 总楼层 建筑年份 是否满二满五 小区ID 建筑类型

gen year_month = substr(date_transaction, 1, 4) + "-" + substr(date_transaction, 6, 2) 
gen year_month_date = date(year_month, "YM")
gen ln_price_unit = ln(price_unit)

sort district date_transaction

gen mean_price_unit = mean(price_unit)

if district == "金牛"{twoway(scatter price_unit year_month)}

