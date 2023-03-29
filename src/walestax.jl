using CSV, DataFrames
using Formatting
using WalesTaxation
using StatsBase

using ScottishTaxBenefitModel
using .LocalLevelCalculations
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

const INCLUDE_OCCUP = true
const INCLUDE_HOUSING = true
const INCLUDE_CT = true
const INCLUDE_HCOMP = true
const INCLUDE_EMPLOYMENT = true

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


function get_system( ; year = 2022 ) :: TaxBenefitSystem
    sys = nothing
    if year == 2022
        sys = load_file("$(MODEL_PARAMS_DIR)/sys_2022-23.jl" )
        ## wales specific CT rels; see []??
        sys.loctax.ct.relativities = Dict{CT_Band,Float64}(
            Band_A=>240/360,
            Band_B=>280/360,
            Band_C=>320/360,
            Band_D=>360/360,
            Band_E=>440/360,
            Band_F=>520/360,                                                                      
            Band_G=>600/360,
            Band_H=>720/360,
            Band_I=>840/360,
            Household_not_valued_separately => 0.0 ) 

        
        # load_file!( sys, "$(MODEL_PARAMS_DIR)/sys_2022-23-july-ni.jl" )
    end 
    weeklyise!(sys)
    return sys
end

 
"""
Very simple implementation of the CT scheme
note this doesn't include rebates apart from single
person rebate
"""
function l_calc_council_tax( 
    hh :: Household{RT}, 
    intermed :: MTIntermediate,
    ctsys :: CouncilTax{RT} ) :: RT where RT 
    ctres = zero(RT)
    if hh.region != Wales
        @assert hh.ct_band != Band_I # We're not Welsh
    end
    ctres = ctsys.band_d[hh.council]* ctsys.relativities[hh.ct_band]
    if intermed.num_adults == 1
        ctres *= (1-ctsys.single_person_discount)
    end
    ## TODO disabled discounts. See CT note.
    return ctres
end



"""
Very simple implementation of the CT scheme
note this doesn't include rebates apart from single
person rebate
"""
function l_calc_council_tax( 
    hh :: Household{RT}, 
    intermed :: MTIntermediate,
    band_d   :: Real,
    ctsys :: CouncilTax{RT} ) :: RT where RT 
    ctres = zero(RT)
    ctres = band_d * ctsys.relativities[hh.ct_band]
    if intermed.num_adults == 1
        ctres *= (1-ctsys.single_person_discount)
    end
    ## TODO disabled discounts. See CT note.
    return ctres
end



function calculate_ct()
    ctf = joinpath( DATADIR, "..", "counciltax", "council-tax-levels-23-24-edited.csv")


    wf = joinpath( DATADIR,  "..", "council-weights-2023-4.csv") 
    settings = Settings()
    settings.benefit_generosity_estimates_available = false
    settings.household_name = "model_households_wales"
    settings.people_name    = "model_people_wales"

    ctrates = CSV.File( ctf ) |> DataFrame
    weights = CSV.File( wf ) |> DataFrame

    band_ds = Dict{Symbol,Float64}()
    p = 0
    for r in eachrow(ctrates)
        p += 1
        if p > 1 # skip 1
            band_ds[Symbol(r.code)] = r.D
        end
    end
    
    sys = get_system(year=2022)
    sys.loctax.ct.band_d = band_ds


    @time nhh, num_people, nhh2 = initialise( settings; reset=false )

    revs = DataFrame( 
        code=fill("", 22), 
        ctrev = zeros(22), 
        average_wage=zeros(22), 
        average_se=zeros(22), 
        ft_jobs=zeros(22), 
        semp=zeros(22) )
    p = 0
    for code in ctrates.code[2:end]
        scode = Symbol(code)
        w = weights[!,code]
        p += 1
        band_d = ctrates[(ctrates.code .== code),:D][1]
        ctrev = 0.0
        average_wage = 0.0
        average_se = 0.0
        nearers = 0.0
        nses = 0.0

        for i in 1:nhh
            hh = get_household(i)
            hh.council = scode
            hh.weight = w[i]
            intermed = make_intermediate( 
                hh, sys.lmt.hours_limits,
                sys.age_limits,
                sys.child_limits )
            ct1 = l_calc_council_tax( hh, intermed.hhint, band_d, sys.loctax.ct )
            ct2 = l_calc_council_tax( 
                hh, intermed.hhint, sys.loctax.ct )
            @assert ct1 ≈ ct2
            for (pid,pers) in hh.people
                if pers.employment_status in [
                    Full_time_Employee ]
                    # Part_time_Employee ]
                    nearers += w[i]
                    average_wage += (w[i]*pers.income[wages])
                elseif  pers.employment_status in [
                    Full_time_Self_Employed,
                    Part_time_Self_Employed]
                    average_se += pers.income[self_employment_income]*w[i]
                    nses += w[i]
                end
            end

            ctrev += w[i]*ct2
        end 
        average_se /= nses
        average_wage /= nearers
        revs.code[p] = code
        revs.ctrev[p] = ctrev
        revs.average_wage[p] = average_wage
        revs.average_se[p] = average_se
        revs.ft_jobs[p] = nearers
        revs.semp[p] = nses
    end
    #=
    for code in ctrates.code[2:end]
        f = Formatting.format(revs[code],precision=0, commas=true)
        println( "$code = $(f)")
    end
    =#

    revs
end



function make_target_list( alldata :: DataFrame, code :: AbstractString ) :: Vector
    data = alldata[alldata.code .== code,:][1,:]
    v = zeros(52)
    
    v = initialise_target_dataframe_wales_la(1)[1,:]

    if INCLUDE_HCOMP
        single_person = data."Household composition: One person household; measures: Value" 
        single_parent = data."Household composition: Single family household: Lone parent family: With dependent children; measures: Value" 
        single_family = data."Household composition: Single family household; measures: Value" - single_parent
        v.multi_family = data."Household composition: Other household types; measures: Value"
        ht = single_person + single_parent + single_family + v.multi_family
        println( "hh target = $ht ")

    end

    if INCLUDE_CT
        v.A = data.A
        v.B = data.B
        v.C = data.C
        v.D = data.D
        v.E = data.E
        v.F = data.F
        v.G = data.G
        v.H = data.H
        v.I = data.I       
    end
    v.m_0_4 = data."Sex: Male; Age: Aged 4 years and under; measures: Value"
    v.m_5_9 = data."Sex: Male; Age: Aged 5 to 9 years; measures: Value"
    v.m_10_15 = data."Sex: Male; Age: Aged 10 to 15 years; measures: Value"
    v.m_16_19 = data."Sex: Male; Age: Aged 16 to 19 years; measures: Value"
    v.m_20_24 = data."Sex: Male; Age: Aged 20 to 24 years; measures: Value"
    v.m_25_34 = data."Sex: Male; Age: Aged 25 to 34 years; measures: Value"
    v.m_35_49 = data."Sex: Male; Age: Aged 35 to 49 years; measures: Value"
    v.m_50_64 = data."Sex: Male; Age: Aged 50 to 64 years; measures: Value"
    v.m_65_74 = data."Sex: Male; Age: Aged 65 to 74 years; measures: Value"
    v.m_75_plus = data."Sex: Male; Age: Aged 75 to 84 years; measures: Value" +
        data."Sex: Male; Age: Aged 85 years and over; measures: Value"
    v.f_0_4 = data."Sex: Female; Age: Aged 4 years and under; measures: Value"
    v.f_5_9 = data."Sex: Female; Age: Aged 5 to 9 years; measures: Value"
    v.f_10_15 = data."Sex: Female; Age: Aged 10 to 15 years; measures: Value"
    v.f_16_19 = data."Sex: Female; Age: Aged 16 to 19 years; measures: Value"
    v.f_20_24 = data."Sex: Female; Age: Aged 20 to 24 years; measures: Value"
    v.f_25_34 = data."Sex: Female; Age: Aged 25 to 34 years; measures: Value"
    v.f_35_49 = data."Sex: Female; Age: Aged 35 to 49 years; measures: Value"
    v.f_50_64 = data."Sex: Female; Age: Aged 50 to 64 years; measures: Value"
    v.f_65_74 = data."Sex: Female; Age: Aged 65 to 74 years; measures: Value"
    v.f_75_plus = data."Sex: Female; Age: Aged 75 to 84 years; measures: Value" +
        data."Sex: Female; Age: Aged 85 years and over; measures: Value"
    # v[31] = data."Economic activity status: Economically active (excluding full-time students)" +
    #     data."Economic activity status: Economically active and a full-time student"
    if INCLUDE_EMPLOYMENT 
        v.ft_employed = data."Economic activity status: Economically active (excluding full-time students): In employment: Employee: Full-time" +
            data."Economic activity status: Economically active and a full-time student: In employment: Employee: Full-time"
        
        v.pt_employed = data."Economic activity status: Economically active (excluding full-time students): In employment: Employee: Part-time" +
            data."Economic activity status: Economically active and a full-time student: In employment: Employee: Part-time"
        
        v.selfemp = data."Economic activity status: Economically active (excluding full-time students):In employment:Self-employed with employees" +
            data."Economic activity status: Economically active (excluding full-time students):In employment:Self-employed without employees" +
            data."Economic activity status: Economically active and a full-time student:In employment:Self-employed with employees"+
            data."Economic activity status: Economically active and a full-time student:In employment:Self-employed without employees"
        
        # omit Economic activity status: Economically active (excluding full-time students): Unemployed
    
        
        v.economic_inactive =  data."Economic activity status: Economically inactive"

    end


    if INCLUDE_OCCUP
        

        # ? don't ommit?
        # data."Occupation (current): 1. Managers, directors and senior officials"
        v.Soc_Professional_Occupations =  data."Occupation (current): 2. Professional occupations"
        v.Soc_Associate_Prof_and_Technical_Occupations =  data."Occupation (current): 3. Associate professional and technical occupations"
        v.Soc_Admin_and_Secretarial_Occupations =  data."Occupation (current): 4. Administrative and secretarial occupations"
        v.Soc_Skilled_Trades_Occupations =  data."Occupation (current): 5. Skilled trades occupations"
        v.Soc_Caring_leisure_and_other_service_occupations =  data."Occupation (current): 6. Caring, leisure and other service occupations"
        v.Soc_Sales_and_Customer_Service =  data."Occupation (current): 7. Sales and customer service occupations"
        v.Soc_Process_Plant_and_Machine_Operatives =  data."Occupation (current): 8. Process, plant and machine operatives"
        v.Soc_Elementary_Occupations =  data."Occupation (current): 9. Elementary occupations"
    end
    if INCLUDE_HOUSING 
        # data."Tenure of household: Owned: Owns outright"
        v.mortgaged =  data."Tenure of household: Owned: Owns with a mortgage or loan" +
            data."Tenure of household: Shared ownership: Shared ownership"
        v.council =  data."Tenure of household: Social rented: Rents from council or Local Authority"
        v.other_social_rented =  data."Tenure of household: Social rented: Other social rented"
        v.private_rented_rent_free =  data."Tenure of household: Private rented" + 
            data."Tenure of household: Lives rent free"
        # data."Number of bedrooms: 1 bedroom"
        v.bedrooms_2 =  data."Number of bedrooms: 2 bedrooms"
        v.bedrooms_3 =  data."Number of bedrooms: 3 bedrooms"
        v.bedrooms_4_plus =  data."Number of bedrooms: 4 or more bedrooms"
        v.semi_detached = data."Accommodation type: Semi-detached"
        v.terraced = data."Accommodation type: Terraced"
        v.flat_or_maisonette = data."Accommodation type: In a purpose-built block of flats or tenement"
        v.converted_flat = data."Accommodation type: Part of a converted or shared house, including bedsits"    
        v.other_accom = 
            data."Accommodation type: Part of another converted building, for example, former school, church or warehouse" +
            data."Accommodation type: In a commercial building, for example, in an office building, hotel or over a shop" +
            data."Accommodation type: A caravan or other mobile or temporary structure"            
    end
    out = Vector(v)
    for i in eachindex(out)
        @assert out[i] !== 0 "element $i is zero"
    end
    println( "out=$out")
    return out
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

function initialise_target_dataframe_wales_la( n :: Integer ) :: DataFrame
    d = DataFrame()

    if INCLUDE_HCOMP 
        # d.single_person = zeros(n) #1
        # d.single_parent = zeros(n) # 2
        # d.single_family = zeros(n) # 3
        d.multi_family = zeros(n) # 4
    end

    if INCLUDE_CT
        d.A = zeros(n) #7
        d.B = zeros(n) #5
        d.C = zeros(n) #6
        d.D = zeros(n)
        d.E = zeros(n) #8
        d.F = zeros(n) #9
        d.G = zeros(n) # 10
        d.H = zeros(n) # 11
        d.I = zeros(n) # 12
    end

    d.m_0_4 = zeros(n)
    d.m_5_9 = zeros(n)
    d.m_10_15 = zeros(n) # note uneven gaps here
    d.m_16_19 = zeros(n)
    d.m_20_24 = zeros(n)
    d.m_25_34 = zeros(n)
    d.m_35_49 = zeros(n)
    d.m_50_64 = zeros(n)
    d.m_65_74 = zeros(n)
    d.m_75_plus = zeros(n)
    d.f_0_4 = zeros(n)
    d.f_5_9 = zeros(n)
    d.f_10_15 = zeros(n) # note uneven gaps here
    d.f_16_19 = zeros(n)
    d.f_20_24 = zeros(n)
    d.f_25_34 = zeros(n)
    d.f_35_49 = zeros(n)
    d.f_50_64 = zeros(n)
    d.f_65_74 = zeros(n)
    d.f_75_plus = zeros(n)

    if INCLUDE_EMPLOYMENT
        d.ft_employed  = zeros(n)
        d.pt_employed = zeros(n)
        d.selfemp = zeros(n)
    end

    if INCLUDE_OCCUP 
        d.economic_inactive = zeros(n)

        d.Soc_Professional_Occupations = zeros(n)	#	83	% all in employment who are - 2: professional occupations (SOC2010)
        d.Soc_Associate_Prof_and_Technical_Occupations = zeros(n)	#	84	% all in employment who are - 3: associate prof & tech occupations (SOC2010)
        d.Soc_Admin_and_Secretarial_Occupations = zeros(n)	#	85	% all in employment who are - 4: administrative and secretarial occupations (SOC2010)
        d.Soc_Skilled_Trades_Occupations = zeros(n)	#	86	% all in employment who are - 5: skilled trades occupations (SOC2010)
        d.Soc_Caring_leisure_and_other_service_occupations = zeros(n)	#	87	% all in employment who are - 6: caring, leisure and other service occupations (SOC2010)
        d.Soc_Sales_and_Customer_Service = zeros(n)	#	88	% all in employment who are - 7: sales and customer service occupations (SOC2010)
        d.Soc_Process_Plant_and_Machine_Operatives = zeros(n)  	#	89	% all in employment who are - 8: process, plant and machine operatives (SOC2010)
        d.Soc_Elementary_Occupations = zeros(n)    #   90  % all in employment who are - 9: elementary occupations (SOC2010) 
    end

    if INCLUDE_HOUSING 
        # owner_occupied = zeros(n),
        d.mortgaged = zeros(n)
        # d.shared_ownership = zeros(n)
        d.council = zeros(n)
        d.other_social_rented = zeros(n)
        d.private_rented_rent_free = zeros(n)

        # one bedroom
        d.bedrooms_2 = zeros(n)
        d.bedrooms_3 = zeros(n)
        d.bedrooms_4_plus = zeros(n)

        # detached
        d.semi_detached = zeros(n)
        d.terraced = zeros(n)
        d.flat_or_maisonette = zeros(n)
        d.converted_flat = zeros(n)
        d.other_accom = zeros(n)
    end
    return d    
end



function make_target_row_wales_la!( 
    row :: DataFrameRow, 
    hh :: Household )

    
    if INCLUDE_HCOMP
        bus = get_benefit_units( hh )
        if is_single(hh)
            # println("single_person")
            single_person = 1
            # 
        elseif size(bus)[1] > 1
            row.multi_family = 1 
            # println( "multi-family")
        elseif is_lone_parent(hh) # only dependent children
            single_parent = 1
            # println( "single_parent")
        else
            single_family = 1 
            # println( "single_family")
            # 
        end
    end

    if INCLUDE_CT
        # println( "hh.ct_band $(hh.ct_band)")
        if hh.ct_band == Band_A
            row.A = 1
        elseif hh.ct_band == Band_B
            row.B = 1
        elseif hh.ct_band == Band_C
            row.C = 1
        elseif hh.ct_band == Band_D
            row.D = 1
        elseif hh.ct_band == Band_E
            row.E = 1
        elseif hh.ct_band == Band_F
            row.F = 1
        elseif hh.ct_band == Band_G
            row.G = 1
        elseif hh.ct_band == Band_H
            row.H = 1
        elseif hh.ct_band == Band_I
            row.I = 1
        else hh.ct_band == Household_not_valued_separately
            row.A = 1 # DODGY!! FIXME
            #
            # @assert false "NO CT BAND"
        end            
    end # CT

    for (pid,pers) in hh.people
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

    if INCLUDE_EMPLOYMENT
        if pers.employment_status == Full_time_Employee
            row.ft_employed += 1
        elseif pers.employment_status == Part_time_Employee
            row.pt_employed += 1
        elseif pers.employment_status in [
            Full_time_Self_Employed,
            Part_time_Self_Employed ]
            row.selfemp += 1
        end
    end

    if INCLUDE_OCCUP
        if pers.employment_status in [
            Full_time_Employee,
            Part_time_Employee,
            Full_time_Self_Employed,
            Part_time_Self_Employed,
            Unemployed
            ]

            # dropped colinear row.economic_active += 1
        else
            row.economic_inactive += 1
        end # active
        if pers.employment_status in [
                Full_time_Employee,
                Part_time_Employee,
                Full_time_Self_Employed,
                Part_time_Self_Employed
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
            end # occupation
        end # include occ
    end # pers loop

    if INCLUDE_HOUSING
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
            row.mortgaged = 1
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
end

function weight_to_la( 
    settings :: Settings,
    alldata :: DataFrame, 
    code :: AbstractString,
    num_households :: Int )
    targets = make_target_list( alldata, code ) 
    hhtotal = alldata[alldata.code .== code,:][1,:]."Household composition: Total; measures: Value"
    println( "calculating for $code; hh total $hhtotal")
    weights = generate_weights(
        num_households;
        weight_type = settings.weight_type,
        lower_multiple = settings.lower_multiple, # these values can be narrowed somewhat, to around 0.25-4.7
        upper_multiple = settings.upper_multiple,
        household_total = hhtotal,
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

function t_make_target_dataset( nhhlds :: Integer, 
    initialise_target_dataframe :: Function,
    make_target_row! :: Function ) :: Matrix
    df :: DataFrame = initialise_target_dataframe( nhhlds )
    for hno in 1:nhhlds
        hh = FRSHouseholdGetter.get_household( hno )
        make_target_row!( df[hno,:], hh )
    end
    m = Matrix{Float64}(df) 

    # consistency
    nr,nc = size(m)
    # no column is all zero - since only +ive cells possible this is the easiest way
    for c in 1:nc 
        @assert sum(m[:,c]) != 0 "all zero column $c"
    end
    # no row all zero
    for r in 1:nr
        @assert sum(m[r,:] ) != 0 "all zero row $r"
    end
    return m
end
 
function weight_councils()
    data = load_all_census()
    settings = Settings()
    settings.benefit_generosity_estimates_available = false
    settings.household_name = "model_households_wales"
    settings.people_name    = "model_people_wales"
    settings.lower_multiple = 0.01
    settings.upper_multiple = 40.0
    settings.auto_weight = false

    @time nhh, num_people, nhh2 = initialise( settings; reset=false )
    nrs = nhh*length(data.code)

    d = DataFrame( 
        hid=fill(0, nrs ), 
        data_year = fill(0, nrs ),
        code = fill( "", nrs ),
        weight = zeros( nrs )
    )
    
    p = 0

    for code in data.code
        if code in ["W06000005", "W06000024","W06000019","W06000012", "W06000021", "W06000014", "W06000015"] # larger no of band A cts so loosen
            settings.lower_multiple = 0.01
            settings.upper_multiple = 40.0
        else
            settings.lower_multiple = 0.025
            settings.upper_multiple = 30.0
        end
        w = weight_to_la( settings,
            data,
            code,
            nhh )
        for n in 1:nhh
            mhh = get_household( n )
            p += 1
            r = d[p,:]
            r.hid = mhh.hid
            r.data_year = mhh.data_year
            r.weight = w[n]
            r.code = code
        end 
    end
    du = unstack(d, :code, :weight )
    CSV.write( "/home/graham_s/tmp/council-weights-2023-4.csv", du )

end