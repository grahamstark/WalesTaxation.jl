
using WalesTaxation
using ScottishTaxBenefitModel
using .Definitions
using .ModelHousehold
using .FRSHouseholdGetter
using .Weighting
using .RunSettings
using DataFrames

# includet( (joinpath(dirname(pathof(WalesTaxation)),"..", "src", "walestax.jl")))

data = load_all_census()

cts = CSV.File( "data/local/council_tax_bands.csv")|> DataFrame
data = leftjoin(data,cts[2:end,:],on=:name)
for b in 'A':'I'
    sb = Symbol(b)
    data[!,sb] = round.((data[!,sb] .* data."Household composition: Total; measures: Value")/100)
end

for n in unique(rdata.variable  )
    println(n)
end

## FIXME more detail

#=
Household composition: Single family household; measures: Value
Household composition: Single family household: Lone parent family: With dependent children; measures: Value
Household composition: Other household types; measures: Value
Household composition: Other household types: With dependent children; measures: Value
=#

data."Sex: Male; Age: Aged 4 years and under; measures: Value"
data."Sex: Male; Age: Aged 5 to 9 years; measures: Value"
data."Sex: Male; Age: Aged 10 to 15 years; measures: Value"
data."Sex: Male; Age: Aged 16 to 19 years; measures: Value"
data."Sex: Male; Age: Aged 20 to 24 years; measures: Value"
data."Sex: Male; Age: Aged 25 to 34 years; measures: Value"
data."Sex: Male; Age: Aged 35 to 49 years; measures: Value"
data."Sex: Male; Age: Aged 50 to 64 years; measures: Value"
data."Sex: Male; Age: Aged 65 to 74 years; measures: Value"
data."Sex: Male; Age: Aged 75 to 84 years; measures: Value" +
data."Sex: Male; Age: Aged 85 years and over; measures: Value"
data."Sex: Female; Age: Aged 4 years and under; measures: Value"
data."Sex: Female; Age: Aged 5 to 9 years; measures: Value"
data."Sex: Female; Age: Aged 10 to 15 years; measures: Value"
data."Sex: Female; Age: Aged 16 to 19 years; measures: Value"
data."Sex: Female; Age: Aged 20 to 24 years; measures: Value"
data."Sex: Female; Age: Aged 25 to 34 years; measures: Value"
data."Sex: Female; Age: Aged 35 to 49 years; measures: Value"
data."Sex: Female; Age: Aged 50 to 64 years; measures: Value"
data."Sex: Female; Age: Aged 65 to 74 years; measures: Value"
data."Sex: Female; Age: Aged 75 to 84 years; measures: Value" +
    data."Sex: Female; Age: Aged 85 years and over; measures: Value"


data.econ_economic_active = data."Economic activity status: Economically active (excluding full-time students)" +
    data."Economic activity status: Economically active and a full-time student"
    
data.econ_economic_inactive = data."Economic activity status: Economically inactive"

data."Occupation (current): 1. Managers, directors and senior officials"
data."Occupation (current): 2. Professional occupations"
data."Occupation (current): 3. Associate professional and technical occupations"
data."Occupation (current): 4. Administrative and secretarial occupations"
data."Occupation (current): 5. Skilled trades occupations"
data."Occupation (current): 6. Caring, leisure and other service occupations"
data."Occupation (current): 7. Sales and customer service occupations"
data."Occupation (current): 8. Process, plant and machine operatives"
data."Occupation (current): 9. Elementary occupations"
# NA?

# data."Tenure of household: Owned: Owns outright"
data."Tenure of household: Owned: Owns with a mortgage or loan"
data."Tenure of household: Shared ownership: Shared ownership"
data."Tenure of household: Social rented: Rents from council or Local Authority"
data."Tenure of household: Social rented: Other social rented"
data."Tenure of household: Private rented"
data."Tenure of household: Lives rent free"

# data."Number of bedrooms: 1 bedroom"
data."Number of bedrooms: 2 bedrooms"
data."Number of bedrooms: 3 bedrooms"
data."Number of bedrooms: 4 or more bedrooms"

# data."Accommodation type: Total: All households"
# data."Accommodation type: Detached"
data."Accommodation type: Semi-detached"
data."Accommodation type: Terraced"
data."Accommodation type: In a purpose-built block of flats or tenement"
data."Accommodation type: Part of a converted or shared house, including bedsits"

data.accom_other = 
    data."Accommodation type: Part of another converted building, for example, former school, church or warehouse" +
    data."Accommodation type: In a commercial building, for example, in an office building, hotel or over a shop" +
    data."Accommodation type: A caravan or other mobile or temporary structure"

#=
detatched = 1
semi_detached = 2
terraced = 3
flat_or_maisonette = 4
converted_flat = 5
caravan = 6
other_dwelling = 7
=#

#=
data.econ_employed =
    data."Economic activity status: Economically active (excluding full-time students):In employment:Employee" +
    data."Economic activity status: Economically active and a full-time student:In employment:Employee"

data.econ_selfemp = 
    data."Economic activity status: Economically active (excluding full-time students):In employment:Self-employed with employees" +
    data."Economic activity status: Economically active (excluding full-time students):In employment:Self-employed without employees" +
    data."Economic activity status: Economically active and a full-time student:In employment:Self-employed with employees" +
    data."Economic activity status: Economically active and a full-time student:In employment:Self-employed without employees"

data.econ_unemployed = 
    data."Economic activity status: Economically active (excluding full-time students): Unemployed" +
    data."Economic activity status: Economically active and a full-time student: Unemployed"

data."Economic activity status: Economically inactive: Retired"
data."Economic activity status: Economically inactive: Student"
data."Economic activity status: Economically inactive: Looking after home or family"
data."Economic activity status: Economically inactive: Long-term sick or disabled"
data."Economic activity status: Economically inactive: Other"
  
data."Economic activity status: Total: All usual residents aged 16 years and over" .== 
    (data.econ_employed + data.econ_selfemp + data.econ_unemployed +
    data."Economic activity status: Economically inactive: Retired" +
    data."Economic activity status: Economically inactive: Student" +
    data."Economic activity status: Economically inactive: Looking after home or family" +
    data."Economic activity status: Economically inactive: Long-term sick or disabled" +
    data."Economic activity status: Economically inactive: Other")


data.econ_employed =
    data."Economic activity status: Economically active (excluding full-time students):In employment:Employee" 

data.econ_selfemp = 
    data."Economic activity status: Economically active (excluding full-time students):In employment:Self-employed with employees" +
    data."Economic activity status: Economically active (excluding full-time students):In employment:Self-employed without employees" 

data.econ_unemployed = 
    data."Economic activity status: Economically active (excluding full-time students): Unemployed" 
=#


rdata = stack( data, Not( [:date,:name,:code]))

# this works
data."Economic activity status: Economically active and a full-time student" + 
        data."Economic activity status: Economically active (excluding full-time students)" + 
        data."Economic activity status: Economically inactive: Retired" +
        data."Economic activity status: Economically inactive: Student" +
        data."Economic activity status: Economically inactive: Looking after home or family" +
        data."Economic activity status: Economically inactive: Long-term sick or disabled" +
        data."Economic activity status: Economically inactive: Other"


data."Economic activity status: Economically active and a full-time student"        

data."Economic activity status: Economically active and a full-time student:In employment:Employee" + 
        data."Economic activity status: Economically active and a full-time student:In employment:Self-employed with employees" +
        data."Economic activity status: Economically active and a full-time student:In employment:Self-employed without employees" + 
        data."Economic activity status: Economically active and a full-time student: Unemployed"

data."Economic activity status: Economically active (excluding full-time students)" 

data."Economic activity status: Economically active (excluding full-time students):In employment:Employee" +
    data."Economic activity status: Economically active (excluding full-time students):In employment:Self-employed with employees" +
    data."Economic activity status: Economically active (excluding full-time students):In employment:Self-employed without employees" +
    data."Economic activity status: Economically active (excluding full-time students): Unemployed"

