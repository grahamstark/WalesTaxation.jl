
using WalesTaxation
using ScottishTaxBenefitModel
using .Definitions
using .ModelHousehold
using .FRSHouseholdGetter
using .Weighting
using .RunSettings

using DataFrames

for n in names(data)
    println(n)
end

data = WalesTaxation.loadallcensus()
rdata = stack( data, Not( [:date,:name,:code]))

for n in unique(rdata.variable  )
    println(n)
end