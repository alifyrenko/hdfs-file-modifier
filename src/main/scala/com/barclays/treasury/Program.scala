package com.barclays.treasury

import com.barclays.treasury.modifier.ParquetFileProcessor

object Program {

  def main(args: Array[String]): Unit = {
    ParquetFileProcessor().modifyParquetFiles()
  }
}
