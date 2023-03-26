# Write your package code here.

using CSV, DataFrames
using WalesTaxation
using ScottishTaxBenefitModel
using .Definitions
using .ModelHousehold
using .FRSHouseholdGetter
using .Weighting
using .RunSettings


export load_all_census,
    copysbdata,
    create_target_matrix,
    get_run_settings,
    DATADIR

const DATADIR=(joinpath(dirname(pathof(WalesTaxation)),"..", "data", "local", "census-2021", "la"))

const PACKAGE_SCOTDIR = joinpath(dirname(pathof(ScottishTaxBenefitModel)),"..", "data" )

const ACTUAL_SCOTDIR="/home/graham_s/julia/vw/ScottishTaxBenefitModel/data"

const targets =  ["001", "003", "009", "041", "044", "050", "051","054","059","060","063","066","067"]

"""
Make a copy of Scotben's data directory into some ScottishTaxBenefitModel
package directory 
"""
function copysbdata()
    cp(ACTUAL_SCOTDIR, PACKAGE_SCOTDIR, force=true )
end

function dsum( d::DataFrameRow, names ... )
    t = 0
    for s in names
        sy = Symbol(s)
        t += d[s]
    end
    t
end

function get_run_settings()
    settings = Settings()
    settings.benefit_generosity_estimates_available = false
    settings.household_name = "model_households_wales"
    settings.people_name    = "model_people_wales"
    settings.lower_multiple = 0.2
    settings.upper_multiple = 5.0
    settings.auto_weight = false
    settings
end

function create_weights(targets::DataFrame)::Vector
    @time nhhx, num_peoplex, nhh2x = initialise( settings; reset=true )
    for  hno in 1:nhhx
        hh = get_household(hno)
    end
end

"""
Load one census CSV into a dataframe
"""
function load_one_census( id :: String ) :: DataFrame
    w = CSV.File( joinpath(DATADIR, "census2021-ts$(id)-ltla.csv")) |> DataFrame  
    rename!( w, [:"geography code"=>:"code", :"geography"=>:"name"])
    w[startswith.(w.code,"W"),:]
end

function create_target_matrix( data :: DataFrame ) :: Vector


end

function load_all_census()::DataFrame
    data = load_one_census( targets[1])
    for t in targets[2:end]
        d = load_one_census( t )
        data = innerjoin( data, d, on=[:date,:code,:name] )
    end
    data
end
