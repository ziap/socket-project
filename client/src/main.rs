use std::{collections::HashMap, env, fs::{self, File}, io::{self, BufRead, BufReader, Write}, net::TcpStream, path::{Path, PathBuf}, process, str, thread, time::Duration};
use common::{initialize_handlers, priority_list, Chunk, FileList, Packet};

struct Config {
    output_dir: PathBuf,
}

impl Config {
    fn get() -> Self {
        Self {
            output_dir: if let Ok(input_dir) = env::var("OUTPUT_DIR") {
                input_dir.into()
            } else {
                "output".into()
            },
        }
    }
}

fn read_input(input_path: &Path, inverse_map: &HashMap<&str, usize>, out: &mut [u8]) {
    if let Ok(input_file) = File::open(input_path) {
        for line in BufReader::new(input_file).lines() {
            if let Ok(line) = line {
                let mut iter = line.split_whitespace();
                if let Some(filename) = iter.next() {
                    if let Some(idx) = inverse_map.get(filename) {
                        out[*idx] = match iter.next() {
                            Some("NORMAL")   => 1,
                            Some("HIGH")     => 4,
                            Some("CRITICAL") => 10,
                            _                => continue
                        };
                    }
                }
            }
        }
    }
}

fn format_size(mut x: u64) -> String {
    let suffixes = ["B", "KB", "MB", "GB"];
    let mut current = 0;
    while current + 1 < suffixes.len() && x >= 1024 {
        x /= 1024;
        current += 1;
    }

    format!("{x}{}", suffixes[current])
}

fn main() -> io::Result<()> {
    let opt = Config::get();
    let addr = {
        let mut addr = String::new();
        let mut stdout = std::io::stdout();
        stdout.write_all("Enter the server address: ".as_bytes())?;
        stdout.flush()?;
        std::io::stdin().read_line(&mut addr)?;
        addr.truncate(addr.trim_end().len());
        addr
    };
    
    let mut arg_iter = env::args();
    arg_iter.next();

    let input_path: PathBuf = if let Some(path) = arg_iter.next() {
        path.into()
    } else {
        "input.txt".into()
    };

    let output_path = Path::new(&opt.output_dir);
    if !output_path.exists() {
        fs::create_dir(output_path)?;
    } else {
        if !output_path.is_dir() {
            eprintln!("ERROR: Can't create output directory!");
            process::exit(1);
        }
    }

    println!("Connecting to server at `{addr}`... ");
    let mut stream = TcpStream::connect(addr)?;

    println!("Connection established\n");
    let downloadables = FileList::recv(&mut stream)?;

    let paths: Box<[PathBuf]> = downloadables.iter()
        .map(|(name, _)| output_path.join(name.as_ref()))
        .collect();

    let inverse_map: HashMap<&str, usize> = downloadables.iter()
        .enumerate()
        .map(|(idx, (name, _))| (name.as_ref(), idx))
        .collect();

    println!("Files available for download:");
    for (name, size) in downloadables.iter() {
        println!(" - {name} - {}", format_size(*size));
    }
    println!();

    let mut files = initialize_handlers(downloadables.len());
    let mut priorities = priority_list::new(downloadables.len());
    let mut next_priorities = priority_list::new(downloadables.len());

    let mut progress: Box<[u64]> = vec![0; downloadables.len()].into();

    loop {
        read_input(&input_path, &inverse_map, &mut next_priorities);
        let mut to_download = priority_list::merge(&mut priorities, &next_priorities);
        stream.write_all(&priorities)?;


        // TODO: Terminate this loop with CTRL-C
        while to_download > 0 {
            for idx in 0..downloadables.len() {
                let handler = &mut files[idx];
                let priority = priorities[idx];
                if priority == 0 || handler.done {
                    continue;
                }

                let opened = handler.file.get_or_insert_with(|| {
                    File::create(&paths[idx]).unwrap()
                });

                for _ in 0..priority {
                    let chunk = Chunk::recv(&mut stream)?;
                    progress[idx] += chunk.len as u64;

                    if chunk.write(opened)? {
                        handler.done = true;
                        drop(handler.file.take());
                        println!("Finished downloading `{}`", downloadables[idx].0);
                        to_download -= 1;
                        break;
                    };
                }
            }

            let mut downloading = 0;
            for idx in 0..downloadables.len() {
                if files[idx].done || progress[idx] == 0 {
                    continue;
                }
                let (name, size) = &downloadables[idx];
                println!("Downloading file `{name}` - {}%", progress[idx] * 100 / size);
                downloading += 1;
            }

            for _ in 0..downloading {
                print!("\x1b[A\x1b[K");
            }
        }

        thread::sleep(Duration::from_secs(2));
    }
}
