package social;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

@Configuration
@ImportResource("classpath:/META-INF/spring/hbase-context.xml")
public class HBaseConfig {
}