package com.longrise.dwchart;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class DwChartApplication {

	public static void main(String[] args) {
		SpringApplication.run(DwChartApplication.class, args);
	}

}
