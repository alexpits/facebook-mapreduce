package social;

import java.util.Arrays;
import java.util.Optional;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

	private static final String HADOOP_HOME = "hadoop.home.dir";
	private static final String HADOOP_CMD = "-Hadoop.home.dir=";

	public static void main(String[] args) {
		Optional<String> hadoopHome = Arrays.asList(args).stream().filter(arg -> arg.startsWith(HADOOP_CMD))
				.findFirst();
		if (hadoopHome.isPresent()) {
			System.setProperty(HADOOP_HOME, hadoopHome.get().replace(HADOOP_CMD, ""));
		}
		SpringApplication.run(Application.class, args);
	}

}