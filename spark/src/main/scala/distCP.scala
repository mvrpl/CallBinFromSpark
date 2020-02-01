import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import java.net.URI

case class Args(sourceURI: String = null, destinationURI: String = null, overwrite: Boolean = false)

object UriValid {
  def unapply(in: URI) = Some((
    in.getScheme, 
    in.getHost, 
    in.getPort,
    in.getPath
  ))
}

object DistCP extends App {
    val parserArgs = new scopt.OptionParser[Args]("distCP") {
		opt[String]('s', "sourceURI").required.action( (x, c) => c.copy(sourceURI = x) ).text("Set log level [debug, info, warn, error or fatal].")
		opt[String]('d', "destinationURI").required.action( (x, c) => c.copy(destinationURI = x) ).text("Set log level [debug, info, warn, error or fatal].")
		opt[Unit]("overwrite").action( (_, c) => c.copy(overwrite = true) ).text("Set log level [debug, info, warn, error or fatal].")
	}

    parserArgs.parse(args, Args()).map{config =>
		val spark = SparkSession.builder.getOrCreate
        val sc = spark.sparkContext
        val hadoopFS = FileSystem.get(sc.hadoopConfiguration)

        val binaryCPP = sc.getConf.get("spark.master") match {
            case "yarn" => {
                val binName = sc.getConf.get("spark.yarn.dist.files").split(",")(0)
                "./"++binName.substring(binName.lastIndexOf('/') + 1,binName.length)
            }
            case _ => sc.getConf.get("spark.files").split(",")(0).replace("file://", "")
        }

        val uriSrc = new URI(config.sourceURI)
        val uriDst = new URI(config.destinationURI)

        val srcHadoopFS = uriSrc match {
            case UriValid(scheme, host, port, path) => {
                if (port >= 0 && host != null && scheme != null) {
                    "%s://%s:%d".format(scheme, host, port)
                } else if (port < 0 && host != null && scheme != null) {
                    "%s://%s".format(scheme, host)
                } else {
                    hadoopFS.getUri.toString
                }
            }
        }
        val dstHadoopFS = uriDst match {
            case UriValid(scheme, host, port, path) => {
                if (port >= 0 && host != null && scheme != null) {
                    "%s://%s:%d".format(scheme, host, port)
                } else if (port < 0 && host != null && scheme != null) {
                    "%s://%s".format(scheme, host)
                } else {
                    hadoopFS.getUri.toString
                }
            }
        }

        val paths = hadoopFS.globStatus(new Path(config.sourceURI)).map(uri => new URI(uri.getPath.toString) match {
            case UriValid(_, _, _, path) => path
        })
        val pathsRDD = sc.parallelize(paths)
        println(Seq(binaryCPP, srcHadoopFS, dstHadoopFS, uriDst.getPath))
        val piped = pathsRDD.pipe(Seq(binaryCPP, srcHadoopFS, dstHadoopFS, uriDst.getPath))

        piped.collect.foreach(println)
	} getOrElse {
		System.exit(1)
	}
}