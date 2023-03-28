using CSV, DataFrames
using WalesTaxation
using ScottishTaxBenefitModel
using .Definitions
using .ModelHousehold
using .FRSHouseholdGetter
using .Intermediate
using .Weighting
using .RunSettings
using .STBParameters

export load_all_census,
    copysbdata,
    create_target_matrix,
    get_run_settings,
    DATADIR

const DATADIR=(joinpath(dirname(pathof(WalesTaxation)),"..", "data", "local", "census-2021", "la"))

const PACKAGE_SCOTDIR = joinpath(dirname(pathof(ScottishTaxBenefitModel)),"..", "data" )

const ACTUAL_SCOTDIR="/home/graham_s/julia/vw/ScottishTaxBenefitModel/data"

const targets =  ["001", "003", "009", "041", "044", "050", "051","054","059","060","063","066","067"]

const SYS = get_system( ; year=2022, scotland = false )


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


function get_system( ; year, scotland = true )  :: TaxBenefitSystem
    sys = nothing
    if year == 2022
       sys = load_file("$(MODEL_PARAMS_DIR)/sys_2022-23.jl" )
       # load_file!( sys, "$(MODEL_PARAMS_DIR)/sys_2022-23-july-ni.jl" )
    else
       return getSystem( scotland=scotland )
    end 
    weeklyise!(sys)
    return sys
end

function make_target_list( alldata :: DataFrame, code :: AbstractString ) :: Vector
    data = alldata[alldata.code .== code,:][1,:]
    v = zeros(52)
    
    v = initialise_target_dataframe_wales_la(1)[1,:]

    v.single_person = data."Household composition: One person household; measures: Value" 
    v.single_parent = data."Household composition: Single family household: Lone parent family: With dependent children; measures: Value" 
    v.single_family = data."Household composition: Single family household; measures: Value" - v[2]
    v.multi_family = data."Household composition: Other household types; measures: Value"

    v[5] = data.B
    v[6] = data.C
    v[7] = data.D
    v[8] = data.E
    v[9] = data.F
    v[10] = data.G
    v[11] = data.H
    v[12] = data.I

    # ages drop u5 m 
    v[11] = data."Sex: Male; Age: Aged 4 years and under; measures: Value"
    v[12] = data."Sex: Male; Age: Aged 5 to 9 years; measures: Value"
    v[13] = data."Sex: Male; Age: Aged 10 to 15 years; measures: Value"
    v[14] = data."Sex: Male; Age: Aged 16 to 19 years; measures: Value"
    v[15] = data."Sex: Male; Age: Aged 20 to 24 years; measures: Value"
    v[16] = data."Sex: Male; Age: Aged 25 to 34 years; measures: Value"
    v[17] = data."Sex: Male; Age: Aged 35 to 49 years; measures: Value"
    v[18] = data."Sex: Male; Age: Aged 50 to 64 years; measures: Value"
    v[19] = data."Sex: Male; Age: Aged 65 to 74 years; measures: Value"
    v[20] = data."Sex: Male; Age: Aged 75 to 84 years; measures: Value" +
        data."Sex: Male; Age: Aged 85 years and over; measures: Value"
    v[21] = data."Sex: Female; Age: Aged 4 years and under; measures: Value"
    v[22] = data."Sex: Female; Age: Aged 5 to 9 years; measures: Value"
    v[23] = data."Sex: Female; Age: Aged 10 to 15 years; measures: Value"
    v[24] = data."Sex: Female; Age: Aged 16 to 19 years; measures: Value"
    v[25] = data."Sex: Female; Age: Aged 20 to 24 years; measures: Value"
    v[26] = data."Sex: Female; Age: Aged 25 to 34 years; measures: Value"
    v[27] = data."Sex: Female; Age: Aged 35 to 49 years; measures: Value"
    v[28] = data."Sex: Female; Age: Aged 50 to 64 years; measures: Value"
    v[29] = data."Sex: Female; Age: Aged 65 to 74 years; measures: Value"
    v[30] = data."Sex: Female; Age: Aged 75 to 84 years; measures: Value" +
        data."Sex: Female; Age: Aged 85 years and over; measures: Value"
    # v[31] = data."Economic activity status: Economically active (excluding full-time students)" +
    #     data."Economic activity status: Economically active and a full-time student"
    v[31] =  data."Economic activity status: Economically inactive"
    # ? don't ommit?
    # data."Occupation (current): 1. Managers, directors and senior officials"
    v[32] =  data."Occupation (current): 2. Professional occupations"
    v[33] =  data."Occupation (current): 3. Associate professional and technical occupations"
    v[34] =  data."Occupation (current): 4. Administrative and secretarial occupations"
    v[35] =  data."Occupation (current): 5. Skilled trades occupations"
    v[36] =  data."Occupation (current): 6. Caring, leisure and other service occupations"
    v[37] =  data."Occupation (current): 7. Sales and customer service occupations"
    v[38] =  data."Occupation (current): 8. Process, plant and machine operatives"
    v[39] =  data."Occupation (current): 9. Elementary occupations"
    # data."Tenure of household: Owned: Owns outright"
    v[40] =  data."Tenure of household: Owned: Owns with a mortgage or loan"
    v[41] =  data."Tenure of household: Shared ownership: Shared ownership"
    v[42] =  data."Tenure of household: Social rented: Rents from council or Local Authority"
    v[43] =  data."Tenure of household: Social rented: Other social rented"
    v[44] =  data."Tenure of household: Private rented" + 
        data."Tenure of household: Lives rent free"
    # data."Number of bedrooms: 1 bedroom"
    v[45] =  data."Number of bedrooms: 2 bedrooms"
    v[46] =  data."Number of bedrooms: 3 bedrooms"
    v[47] =  data."Number of bedrooms: 4 or more bedrooms"
    v[48] = data."Accommodation type: Semi-detached"
    v[49] = data."Accommodation type: Terraced"
    v[50] = data."Accommodation type: In a purpose-built block of flats or tenement"
    v[51] = data."Accommodation type: Part of a converted or shared house, including bedsits"    
    v[52] = 
        data."Accommodation type: Part of another converted building, for example, former school, church or warehouse" +
        data."Accommodation type: In a commercial building, for example, in an office building, hotel or over a shop" +
        data."Accommodation type: A caravan or other mobile or temporary structure"            
    return Vector(v[1,:])

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

function get_system( ; year, scotland = true )  :: TaxBenefitSystem
    sys = nothing
    if year == 2022
       sys = load_file("$(MODEL_PARAMS_DIR)/sys_2022-23.jl" )
       # load_file!( sys, "$(MODEL_PARAMS_DIR)/sys_2022-23-july-ni.jl" )
    else
       return getSystem( scotland=scotland )
    end 
    weeklyise!(sys)
    return sys
 end
 


function initialise_target_dataframe_wales_la( n :: Integer ) :: DataFrame
    return DataFrame(

        single_person = zeros(n),
        single_parent = zeros(n),
        single_family = zeros(n),
        multi_family = zeros(n),

        # A
        B = zeros(n),
        C = zeros(n),
        D = zeros(n),
        E = zeros(n),
        F = zeros(n),
        G = zeros(n),
        H = zeros(n),
        I = zeros(n),

        m_0_4 = zeros(n),
        m_5_9 = zeros(n),
        m_10_15 = zeros(n), # note uneven gaps here
        m_16_19 = zeros(n),
        m_20_24 = zeros(n),
        m_25_34 = zeros(n),
        m_35_49 = zeros(n),
        m_50_64 = zeros(n),
        m_65_74 = zeros(n),
        m_75_plus = zeros(n),
        f_0_4 = zeros(n),
        f_5_9 = zeros(n),
        f_10_15 = zeros(n), # note uneven gaps here
        f_16_19 = zeros(n),
        f_20_24 = zeros(n),
        f_25_34 = zeros(n),
        f_35_49 = zeros(n),
        f_50_64 = zeros(n),
        f_65_74 = zeros(n),
        f_75_plus = zeros(n),

        # ?? drop ??
        # economic_active = zeros(n),
        economic_inactive = zeros(n),

        Soc_Professional_Occupations = zeros(n),	#	83	% all in employment who are - 2: professional occupations (SOC2010)
        Soc_Associate_Prof_and_Technical_Occupations = zeros(n),	#	84	% all in employment who are - 3: associate prof & tech occupations (SOC2010)
        Soc_Admin_and_Secretarial_Occupations = zeros(n),	#	85	% all in employment who are - 4: administrative and secretarial occupations (SOC2010)
        Soc_Skilled_Trades_Occupations = zeros(n),	#	86	% all in employment who are - 5: skilled trades occupations (SOC2010)
        Soc_Caring_leisure_and_other_service_occupations = zeros(n),	#	87	% all in employment who are - 6: caring, leisure and other service occupations (SOC2010)
        Soc_Sales_and_Customer_Service = zeros(n),	#	88	% all in employment who are - 7: sales and customer service occupations (SOC2010)
        Soc_Process_Plant_and_Machine_Operatives = zeros(n),  	#	89	% all in employment who are - 8: process, plant and machine operatives (SOC2010)
        Soc_Elementary_Occupations = zeros(n),     #   90  % all in employment who are - 9: elementary occupations (SOC2010) 

        # owner_occupied = zeros(n),
        mortgaged = zeros(n),
        shared_ownership = zeros(n),
        council = zeros(n),
        other_social_rented = zeros(n),
        private_rented_rent_free = zeros(n),

        # one bedroom
        bedrooms_2 = zeros(n),
        bedrooms_3 = zeros(n),
        bedrooms_4_plus = zeros(n),

        # detached
        semi_detached = zeros(n),
        terraced = zeros(n),
        flat_or_maisonette = zeros(n),
        converted_flat = zeros(n),
        other_accom = zeros(n)
    )
end


function make_target_row_wales_la!( 
    row :: DataFrameRow, 
    hh :: Household )

    hhinter = make_intermediate(
        hh,
        SYS.hours_limits, 
        SYS.age_limits,
        SYS.child_limits
        )
    
    if hhinter.hhint.num_benefit_units == 1
        if hhinter.num_people == 1
            row.single_person = 1
        elseif ! hhinter.buint[1].is_sparent # only dependent children
            row.single_parent = 1
        else
            row.single_family = 1 
        end
    else
        row.multi_family = 1 
    end

    for (pid,pers) in hh.people
        if pers.employment_status in [
            Full_time_Employee,
            Part_time_Employee,
            Full_time_Self_Employed,
            Part_time_Self_Employed,
            Unemployed
            ]
            row.economic_active += 1
        else
            row.economic_inactive += 1
        end
        if pers.sex == Male
            if pers.age <= 4
                row.m_0_4 += 1
            elseif pers.age <= 9
                row.m_5_9 += 1
            elseif pers.age <= 15
                row.m_10_15 += 1
            elseif pers.age <= 19
                row.m_16_19 += 1
            elseif pers.age <= 24
                row.m_20_24 += 1
            elseif pers.age <= 34
                row.m_25_34 += 1
            elseif pers.age <= 49
                row.m_35_49 += 1
            elseif pers.age <= 64
                row.m_50_64 += 1
            elseif pers.age <= 74
                row.m_65_74 += 1
            else
                row.m_75_plus += 1
            end

        else  # female
            if pers.age <= 4
                row.f_0_4 += 1
            elseif pers.age <= 9
                row.f_5_9 += 1
            elseif pers.age <= 15
                row.f_10_15 += 1
            elseif pers.age <= 19
                row.f_16_19 += 1
            elseif pers.age <= 24
                row.f_20_24 += 1
            elseif pers.age <= 34
                row.f_25_34 += 1
            elseif pers.age <= 49
                row.f_35_49 += 1
            elseif pers.age <= 64
                row.f_50_64 += 1
            elseif pers.age <= 74
                row.f_65_74 += 1
            else
                row.f_75_plus += 1
            end

        end # female
        #=
        if get(pers.income,attendance_allowance,0.0) > 0 ### sp!!!!!
            row.aa += 1
        end
        if get(pers.income,carers_allowance,0.0) > 0
            row.ca += 1
        end
        if get( pers.income, personal_independence_payment_daily_living, 0.0 ) > 0 ||
           get( pers.income, personal_independence_payment_mobility, 0.0 ) > 0 ||
           get( pers.income, dlaself_care, 0.0 ) > 0 ||
           get( pers.income, dlamobility, 0.0 ) > 0
           row.pip_or_dla += 1
       end
       =#
       if pers.employment_status in [
            Full_time_Employee,
            Part_time_Employee,
            Full_time_Self_Employed,
            Part_time_Self_Employed,
            Unemployed
            ]      
            p = pers.occupational_classification      
            @assert p in [
                Undefined_SOC, ## THIS SHOULD NEVER HAPPEN, but does
                Managers_Directors_and_Senior_Officials,
                Professional_Occupations,
                Associate_Prof_and_Technical_Occupations,
                Admin_and_Secretarial_Occupations,
                Skilled_Trades_Occupations,
                Caring_leisure_and_other_service_occupations,
                Sales_and_Customer_Service,
                Process_Plant_and_Machine_Operatives,
                Elementary_Occupations
            
            ] "$p not recognised hhld $(hh.hid) $(hh.data_year) pid $(pers.pid)"
            # FIXME HACK
            if p == Undefined_SOC
                println( "undefined soc for working person pid $(pers.pid)")
                p = Elementary_Occupations
            end
            if p != Managers_Directors_and_Senior_Officials
                psoc = Symbol( "Soc_$(p)")            
                row[psoc] += 1
            end
       end
    end


    if hh.tenure == Council_Rented
        row.council = 1
    elseif hh.tenure == Housing_Association
        row.other_social_rented = 1
    elseif hh.tenure in [Private_Rented_Unfurnished,
        Private_Rented_Furnished,
        Rent_free,
        Squats ]
        row.private_rented_rent_free = 1
    elseif hh.tenure in [Mortgaged_Or_Shared]
        row.shared_ownership = 1
    elseif hh.tenure == Owned_outright
        # row.
    end

    if hh.bedrooms == 1
        #
    elseif hh.bedrooms == 2
        row.bedrooms_2 = 1
    elseif hh.bedrooms == 3
        row.bedrooms_3 = 1
    else
        row.bedrooms_4_plus = 1
    end
    # dwell_na = -1
    if hh.dwelling == detatched
        # 
    elseif hh.dwelling == semi_detached
        row.semi_detached = 1
    elseif hh.dwelling == terraced
        row.terraced = 1
    elseif hh.dwelling == flat_or_maisonette
        row.flat_or_maisonette = 1
    elseif hh.dwelling == converted_flat
        row.converted_flat = 1
    else 
        row.other_accom = 1
    end
end

function weight_to_la( 
    settings :: Settings,
    alldata :: DataFrame, 
    code :: AbstractString,
    num_households :: Int )
    targets = make_target_list( alldata, code ) 
    data = alldata[alldata.code .== code,:][1,:]
    weights = generate_weights(
        num_households;
        weight_type = settings.weight_type,
        lower_multiple = settings.lower_multiple, # these values can be narrowed somewhat, to around 0.25-4.7
        upper_multiple = settings.upper_multiple,
        household_total = data."Household composition: Total; measures: Value",
        targets = targets,
        initialise_target_dataframe = initialise_target_dataframe_wales_la,
        make_target_row! = make_target_row_wales_la! )
    return weights
end

function load_all_census()::DataFrame
    data = load_one_census( targets[1])
    for t in targets[2:end]
        d = load_one_census( t )
        data = innerjoin( data, d, on=[:date,:code,:name] )
    end
    cts = CSV.File( "data/local/council_tax_bands.csv")|> DataFrame
    data = leftjoin(data,cts[2:end,:],on=:name)
    for b in 'A':'I'
        sb = Symbol(b)
        data[!,sb] = round.((data[!,sb] .* data."Household composition: Total; measures: Value")/100)
    end
    data
end

 
function weight_councils()
    data = load_all_census()
    settings = Settings()
    settings.benefit_generosity_estimates_available = false
    settings.household_name = "model_households_wales"
    settings.people_name    = "model_people_wales"
    settings.lower_multiple = 0.2
    settings.upper_multiple = 5.0
    settings.auto_weight = false

    @time nhh, num_people, nhh2 = initialise( settings; reset=false )
    weights = Dict()
    for code in data.code
        weights[code] = weight_to_la( settings,
            data,
            code,
            nhh )
    end
    weights
end