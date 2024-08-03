use std::{fs::File, io::{self, Read, Write}, mem, net::TcpStream, str};

pub trait Packet {
    fn send(&self, stream: &mut TcpStream) -> io::Result<()>;
    fn recv(stream: &mut TcpStream) -> io::Result<Self> where Self: Sized;
}

pub type FileList = Box<[(Box<str>, u64)]>;

impl Packet for FileList {
    fn send(&self, stream: &mut TcpStream) -> io::Result<()> {
        stream.write_all(&self.len().to_be_bytes())?;
        let sizes = self.iter().fold(Vec::new(), |mut a, (_, size)| {
            a.extend(size.to_be_bytes());
            a
        });
        assert!(sizes.len() == self.len() * mem::size_of::<usize>());
        stream.write_all(&sizes)?;

        let names = self.iter()
            .fold(String::new(), |a, (name, _)| a + "\0" + name);

        let bytes = &names.as_bytes()[1..];
        stream.write_all(&bytes.len().to_be_bytes())?;
        stream.write_all(&bytes)
    }

    fn recv(stream: &mut TcpStream) -> io::Result<Self> {
        let len = {
            let mut buf = [0; mem::size_of::<usize>()];
            stream.read_exact(&mut buf)?;
            usize::from_be_bytes(buf)
        };

        let mut buf = vec![0; len * mem::size_of::<u64>()];
        stream.read_exact(&mut buf)?;
        let filesizes = buf.chunks(mem::size_of::<u64>())
            .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()));

        let names_size = {
            let mut buf = [0; mem::size_of::<usize>()];
            stream.read_exact(&mut buf)?;
            usize::from_be_bytes(buf)
        };

        let mut buf = vec![0; names_size];
        stream.read_exact(&mut buf)?;

        let filenames = str::from_utf8(&buf).unwrap()
            .splitn(len, '\0').map(|name| name.into());

        Ok(filenames.zip(filesizes).collect())
    }
}

pub struct Chunk {
    pub len: usize,
    buf: [u8; 1024],
}

impl Chunk {
    pub fn end(&self) -> bool {
        self.len < 1024
    }
    pub fn read(file: &mut File) -> io::Result<Self> {
        let mut buf = [0; 1024];
        let len = file.read(&mut buf)?;
        Ok(Chunk {len, buf})
    }

    pub fn write(mut self, file: &mut File) -> io::Result<bool> {
        file.write_all(&mut self.buf[..self.len])?;
        Ok(self.end())
    }
}

impl Packet for Chunk {
    fn send(&self, stream: &mut TcpStream) -> io::Result<()> {
        let header = if self.end() { (1 << 15) | self.len as u16 } else { 0 };
        stream.write_all(&header.to_be_bytes())?;
        stream.write_all(&self.buf[..self.len])
    }

    fn recv(stream: &mut TcpStream) -> io::Result<Self> {
        let header = {
            let mut buf = [0; mem::size_of::<u16>()];
            stream.read_exact(&mut buf)?;
            u16::from_be_bytes(buf)
        };
        let end = (header >> 15) != 0;
        let mut buf = [0; 1024];
        let len = if end {
            (header as usize) & 0x3ff
        } else {
            1024
        };

        stream.read_exact(&mut buf[..len])?;
        Ok(Chunk { len, buf })
    }
}

pub struct DownloadableFile {
    pub done: bool,
    pub file: Option<File>
}

pub fn initialize_handlers(len: usize) -> Box<[DownloadableFile]> {
    std::iter::repeat_with(|| DownloadableFile { done: false, file: None })
        .take(len).collect()
}

pub mod priority_list {
    pub fn new(len: usize) -> Box<[u8]> {
        vec![0; len].into()
    }

    pub fn merge(current: &mut [u8], other: &[u8]) -> usize {
        assert!(current.len() == other.len());
        let mut modified = 0;
        for (priority, other_priority) in current.iter_mut().zip(other.iter()) {
            if *priority == 0 {
                if *other_priority != 0 {
                    modified += 1;
                    *priority = *other_priority;
                }
            }
        }
        modified
    }
}
