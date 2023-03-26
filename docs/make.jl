using WalesTaxation
using Documenter

DocMeta.setdocmeta!(WalesTaxation, :DocTestSetup, :(using WalesTaxation); recursive=true)

makedocs(;
    modules=[WalesTaxation],
    authors="Graham Stark",
    repo="https://github.com/grahamstark/WalesTaxation.jl/blob/{commit}{path}#{line}",
    sitename="WalesTaxation.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://grahamstark.github.io/WalesTaxation.jl",
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/grahamstark/WalesTaxation.jl",
    devbranch="main",
)
