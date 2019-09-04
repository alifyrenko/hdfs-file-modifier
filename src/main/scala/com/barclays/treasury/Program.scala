package com.barclays.treasury

import com.barclays.treasury.modifier.ParquetFileProcessor

object Program extends App{
  ParquetFileProcessor().modifyParquetFiles()
}
