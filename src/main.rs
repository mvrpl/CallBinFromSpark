use std::io;
use std::rc::Rc;
use std::cell::RefCell;
use hdfs::HdfsFsCache;
use hdfs::HdfsFs;
use hdfs::HdfsUtil;
use std::env;

fn main() {
	let args: Vec<String> = env::args().collect();
	let mut sourcePath = String::new();
	let cacheS = Rc::new(RefCell::new(HdfsFsCache::new()));
	let cacheD = Rc::new(RefCell::new(HdfsFsCache::new()));
	let fsSrc: HdfsFs = cacheS.borrow_mut().get(&args[1]).ok().unwrap();
	let fsDst: HdfsFs = cacheD.borrow_mut().get(&args[2]).ok().unwrap();
	let destPath: String = args[3];
	match io::stdin().read_line(&mut sourcePath) {
		Ok(_) => {
			println!("Copying from '{}' to '{}'", sourcePath, destPath);
			match HdfsUtil.copy(fsSrc, sourcePath, fsDst, destPath){
				Ok(_) => { println!("Copy successful!") },
				Err(error) => { panic!("Copy error: {}", error) }
			}
		}
		Err(error) => println!("error: {}", error),
	}
}