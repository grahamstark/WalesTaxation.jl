
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

rdata = stack( data, Not( [:date,:name,:code]))

for n in unique(rdata.variable  )
    println(n)
end