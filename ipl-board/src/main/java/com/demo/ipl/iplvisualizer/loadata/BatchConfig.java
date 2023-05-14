package com.demo.ipl.iplvisualizer.loadata;

import com.demo.ipl.iplvisualizer.config.JobCompletionNotificationListener;
import com.demo.ipl.iplvisualizer.model.Match;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
//@EnableBatchProcessing
public class BatchConfig {
    //Class is based on sample code available at https://spring.io/guides/gs/batch-processing/

    private final String[] FIELD_NAMES = new String[] { "id", "city", "date", "player_of_match", "venue",
            "neutral_venue", "team1", "team2", "toss_winner", "toss_decision", "winner", "result", "result_margin",
            "eliminator", "method", "umpire1", "umpire2" };

    /**
     * This method is used to read the input data provided as csv file under resources folder.
     * Method parses each line item with enough information to turn it into a InputData .
     * @return
     */
    @Bean
    public FlatFileItemReader<InputData> reader() {
        return new FlatFileItemReaderBuilder<InputData>()
                .name("MatchItemReader")
                .resource(new ClassPathResource("match-data.csv"))
                .delimited()
                .names(FIELD_NAMES)
                .fieldSetMapper(new BeanWrapperFieldSetMapper<InputData>() {{
                    setTargetType(InputData.class);
                }})
                .build();
    }
    /**
     * Creates instance of the processor .
     * @return
     */
    @Bean
    public DataProcessor processor() {
        return new DataProcessor();
    }

    /**
     * This method is used to convert it into a the required output pojo class named Match.
     * This one is aimed at a JDBC destination and automatically gets a copy of the dataSource created by
     * @EnableBatchProcessing. It includes the SQL statement needed to insert a single Match record,
     * driven by Java bean properties.
     * @return
     */
    @Bean
    public JdbcBatchItemWriter<Match> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Match>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO match (id, city, date, player_of_match, venue, team1, team2, toss_winner, toss_decision, match_winner, result, result_margin, umpire1, umpire2) "
                        + " VALUES (:id, :city, :date, :playerOfMatch, :venue, :team1, :team2, :tossWinner, :tossDecision, :matchWinner, :result, :resultMargin, :umpire1, :umpire2)")
                .dataSource(dataSource)
                .build();
    }

    /**
     * This method is the  definition of job and  is made up of steps .
     * Step is defined in another method called step1 which comprises of reading , processing
     * and writing written above .
     *
     * The last bit of batch configuration is a way to get notified when the job completes defined by
     * JobCompletionNotificationListener.
     * @param jobRepository
     * @param listener
     * @param step1
     * @return
     */
    @Bean
    public Job importUserJob(JobRepository jobRepository,
                             JobCompletionNotificationListener listener, Step step1) {
        return new JobBuilder("importUserJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }

    /**
     * In the step definition, you define how much data to write at a time.
     * In this case, it writes up to ten records at a time.
     * Next, you configure the reader, processor, and writer by using the beans injected earlier.
     * @param jobRepository
     * @param transactionManager
     * @param writer
     * @return
     */
    @Bean
    public Step step1(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager, JdbcBatchItemWriter<Match> writer) {
        return new StepBuilder("step1", jobRepository)
                .<InputData, Match> chunk(10, transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }
}
