nextflow.enable.types = true

record Location {
    city: String
    continent: String
}

record Person {
    firstName: String
    lastName: String
    location: Location
}

record PersonWithData {
    person: Person
    datafile: Path
}

workflow {
    main:
    // Create a channel of people:
    people = channel.of(
        record(firstName: 'Alice', lastName: 'Smith', location: record(city: 'London', continent: 'Europe')),
        record(firstName: 'Bob',   lastName: 'Jones', location: record(city: 'Sydney', continent: 'Oceania')),
        record(firstName: 'Carmen', lastName: 'Ruiz', location: record(city: 'Madrid', continent: 'Europe')),
        record(firstName: 'Dev',   lastName: 'Patel', location: record(city: 'Mumbai', continent: 'Asia')),
        record(firstName: 'Emi',   lastName: 'Tanaka', location: record(city: 'Tokyo', continent: 'Asia')),
        record(firstName: 'Femi',  lastName: 'Adeyemi', location: record(city: 'Lagos', continent: 'Africa')),
        record(firstName: 'Greta', lastName: 'Larsen', location: record(city: 'Oslo', continent: 'Europe')),
        record(firstName: 'Amina', lastName: 'Okafor', location: record(city: 'Nairobi', continent: 'Africa')),
        record(firstName: 'Tarik', lastName: 'Benali', location: record(city: 'Casablanca', continent: 'Africa'))
    )

    peopleWithData = MakeDatafile(people)

    byContinent = peopleWithData
        .map { p -> tuple(p.person.location.continent, p.datafile) }
        .groupBy()

    continentFiles = CombineContinent(byContinent)

    publish:
    people = peopleWithData
    continents = continentFiles
}

output {
    people {
        path { p -> "people/${p.person.location.continent}" }
        index {
            path 'people.json'
        }
    }

    continents {
        path 'continents'
    }
}

process MakeDatafile {
    input:
    person: Person

    output:
    record(person: person, datafile: file("${person.firstName}_${person.lastName}.dat"))

    script:
    """
    echo '${person.firstName} ${person.lastName} from ${person.location.city}, ${person.location.continent}' >> ${person.firstName}_${person.lastName}.dat
    """
}

process CombineContinent {
    input:
    tuple(continent: String, datafiles: Bag<Path>)

    output:
    file("${continent}.txt")

    script:
    """
    cat ${datafiles.toSorted { f -> f.name }.join(' ')} > ${continent}.txt
    """
}
