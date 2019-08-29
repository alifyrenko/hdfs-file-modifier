package com.barclays.treasury.traits

import java.io.File
import java.util

trait IFileListRetriever {
  def retrieveFileList(): util.Collection[File]
}
