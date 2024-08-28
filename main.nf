workflow {
    characters = Channel.of ( 
        [
            ['Character', 'Type'],
            ['Zhongli', 'earth'],
            ['Bennet', 'fire'],
            ['Xiangling', 'fire'],
            ['Navia', 'earth']
        ]
    )
    TASK(characters)
    DUCKDB( 
        TASK.out.csv,
        Channel.of("Type\nearth").collectFile()
    ).stdout
        .view()
}

process TASK {
    input:
    val char_tbl

    exec:
    file("${task.workDir}/characters.csv").text = char_tbl*.join(',').join('\n')
    file("${task.workDir}/characters.tsv").text = char_tbl*.join('\t').join('\n')

    output:
    path("characters.csv"), emit: csv
    path("characters.tsv"), emit: tsv
}

process DUCKDB {
    input:
    path csv
    path filter

    container 'community.wave.seqera.io/library/duckdb-cli:1.0.0--a85d12a2a9de17c9'

    script:
    """
    duckdb <<'END_SQL' | tee updated.csv | head
        CREATE TABLE characters AS SELECT #2, #1 FROM '${csv}';
        # DESCRIBE characters;
        CREATE TABLE filter AS SELECT #1 FROM '${filter}';
        COPY( 
            SELECT characters.*
            FROM characters
            JOIN filter
            ON characters.Type = filter.Type
        ) TO '/dev/stdout';
    END_SQL 
    """

    output:
    stdout emit: stdout
    path("updated.csv"), emit: csv
}
