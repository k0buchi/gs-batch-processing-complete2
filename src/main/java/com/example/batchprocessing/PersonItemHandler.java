package com.example.batchprocessing;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.core.io.ClassPathResource;

public class PersonItemHandler implements ItemProcessor<Person, Person>, ItemReader<Person>, ItemWriter<Person> {

	private final FlatFileItemReader<Person> reader;
	private final JdbcBatchItemWriter<Person> writer;
	private static final Logger log = LoggerFactory.getLogger(PersonItemHandler.class);

	public PersonItemHandler(DataSource dataSource) {
	    this.reader = new FlatFileItemReaderBuilder<Person>()
	            .name("personItemReader")
	            .resource(new ClassPathResource("sample-data.csv"))
	            .delimited()
	            .names("firstName", "lastName")
	            .targetType(Person.class)
	            .build();
	    reader.open(new ExecutionContext());

	    this.writer = new JdbcBatchItemWriterBuilder<Person>()
	            .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
	            .dataSource(dataSource)
	            .beanMapped()
	            .build();
	    writer.afterPropertiesSet();
	}
	
	@Override
	public Person process(final Person person) {
		final String firstName = person.firstName().toUpperCase();
		final String lastName = person.lastName().toUpperCase();

		final Person transformedPerson = new Person(firstName, lastName);

		log.info("Converting (" + person + ") into (" + transformedPerson + ")");

		return transformedPerson;
	}

	@Override
	public void write(Chunk<? extends Person> chunk) throws Exception {
	    writer.write(chunk);
	}

	@Override
	public Person read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
	    return reader.read();
	}
}
