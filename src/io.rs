//! Small portable positional-I/O helpers.

use std::fs::File;
use std::io;

/// Read exactly `buf.len()` bytes starting at `pos` without moving any cursor.
pub fn read_exact_at(file: &File, buf: &mut [u8], pos: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, pos)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < buf.len() {
            let n = file.seek_read(&mut buf[done..], pos + done as u64)?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            done += n;
        }
        Ok(())
    }
}
