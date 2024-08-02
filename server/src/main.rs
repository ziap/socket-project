use std::{env, fs::File, io::{self, Read}, net::{TcpListener, TcpStream}, path::{Path, PathBuf}, process, sync::mpsc, thread};
use common::{initialize_handlers, priority_list, Chunk, FileList, Packet};

struct WorkerContext {
    file_list: FileList,
    path_list: Box<[PathBuf]>
}

impl WorkerContext {
    fn new(files: &FileList, paths: &[PathBuf]) -> Self {
        Self {
            file_list: files.clone(),
            path_list: paths.into(),
        }
    }

    fn execute(&self, mut connection: TcpStream) -> io::Result<()> {
        self.file_list.send(&mut connection)?;

        let mut files = initialize_handlers(self.file_list.len());
        let mut priorities = priority_list::new(self.file_list.len());
        let mut next_priorities = priority_list::new(self.file_list.len());

        loop {
            connection.set_read_timeout(None)?;
            connection.read_exact(&mut next_priorities)?;
            let mut to_download = priority_list::merge(&mut priorities, &next_priorities);
            
            // connection.set_read_timeout(Some(Duration::from_micros(20)))?;
            while to_download > 0 {
                for ((handler, path), priority) in files.iter_mut()
                    .zip(self.path_list.iter())
                    .zip(priorities.iter()) {
                    if *priority == 0 || handler.done {
                        continue;
                    }

                    let opened = handler.file.get_or_insert_with(|| {
                        File::open(path).unwrap()
                    });
                    for _ in 0..*priority {
                        let chunk = Chunk::read(opened)?;

                        chunk.send(&mut connection)?;

                        if chunk.end() {
                            handler.done = true;
                            drop(handler.file.take());
                            to_download -= 1;
                            break;
                        }
                    }
                }

                // match connection.read_exact(&mut [0]) {
                //     Ok(()) => {
                //         todo!("Handle file change");
                //     },
                //     Err(err) => {
                //         if err.kind() != io::ErrorKind::WouldBlock {
                //             return Err(err);
                //         }
                //     }
                // }
            }
        }
    }
}

struct Config {
    thread_count: usize,
    ip: Box<str>,
    port: Box<str>,
    input_dir: PathBuf,
}

impl Config {
    fn get() -> Self {
        Self {
            thread_count: if let Ok(count) = env::var("THREAD_COUNT") {
                count.parse().unwrap()
            } else {
                thread::available_parallelism().unwrap().get()
            },
            ip: if let Ok(ip) = env::var("IP") {
                ip.into()
            } else {
                "127.0.0.1".into()
            },
            port: if let Ok(port) = env::var("PORT") {
                port.into()
            } else {
                "3000".into()
            },
            input_dir: if let Ok(input_dir) = env::var("INPUT_DIR") {
                input_dir.into()
            } else {
                "input".into()
            },
        }
    }
}

fn get_files(input_dir: &Path) -> (FileList, Box<[PathBuf]>) {
    let files = match input_dir.read_dir() {
        Ok(files) => files,
        Err(err) => {
            eprintln!("ERROR: Failed to read directory `{}`: {err}", input_dir.display());
            return ([].into(), [].into());
        }
    };

    let iter = files.into_iter().filter_map(|entry| {
        let file = match entry {
            Ok(file) => file.path(),
            Err(err) => {
                eprintln!("ERROR: {err}");
                return None;
            }
        };

        if !file.is_file() {
            return None;
        }

        let name: Box<str> = file.file_name()?.to_str()?.into();
        if name.contains('\0') {
            eprintln!("ERROR: Name `{name}` contains the null-terminator");
            return None;
        }

        let size = match file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(err) => {
                eprintln!("ERROR: Failed to get size of file `{}`: {err}", file.display());
                return None;
            }
        };

        Some(((name, size), file))
    }).unzip();

    let (files, paths): (Vec<_>, Vec<_>) = iter;
    (files.into(), paths.into())
}

fn main() {
    let opt = Config::get();

    let (sender, receiver) = mpsc::channel();
    let mut workers = Vec::with_capacity(opt.thread_count);

    let (files, paths) = get_files(&opt.input_dir);

    for id in 0..opt.thread_count {
        let local_sender = sender.clone();
        let (worker_sender, worker_receiver) = mpsc::channel();

        let ctx = WorkerContext::new(&files, &paths);

        workers.push(worker_sender);
        thread::spawn(move || {
            local_sender.send(id).unwrap();
            while let Ok(job) = worker_receiver.recv() {
                if let Err(err) = ctx.execute(job) {
                    eprintln!("ERROR: Failed to handle connection: {err}")
                }
                local_sender.send(id).unwrap();
            }
        });
    }

    let addr = format!("{}:{}", opt.ip, opt.port);
    let listener = match TcpListener::bind(&addr) {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("ERROR: failed to bind TCP listener: {err}");
            process::exit(1);
        },
    };

    println!("Server listening on: {addr}");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let worker_id = receiver.recv().unwrap();

                if let Ok(addr) = stream.peer_addr() {
                    println!("[Thread {worker_id}] Client `{addr}` connected");
                } else {
                    eprint!("[Thread {worker_id}] Client with unknown address connected");
                }

                workers[worker_id].send(stream).unwrap();
            },
            Err(err) => {
                eprintln!("ERROR: Failed to retrieve incoming stream: {err}");
            }
        }
    }
}
