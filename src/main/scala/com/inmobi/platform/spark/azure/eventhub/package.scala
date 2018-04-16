package com.inmobi.platform.spark.azure

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object eventhub {
  implicit class AzureEventhubDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def azureEventhub: String => Unit = writer.format("com.inmobi.platform.spark.azure.eventhub").save
  }

  implicit class AzureEventhubDataFrameReader(reader: DataFrameReader) {
    def azureEventhub: String => DataFrame = reader.format("com.inmobi.platform.spark.azure.eventhub").load
  }
}
