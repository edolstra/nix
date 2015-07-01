#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <zip.h>

#include <map>
#include <cstring>

#include "source-access.hh"
#include "store-api.hh"
#include "util.hh"

namespace nix {


struct cmp_str
{
    bool operator ()(char const * a, char const * b)
    {
        return std::strcmp(a, b) < 0;
    }
};


struct ZipFile
{
    struct zip * p;
    typedef std::map<const char *, struct zip_stat, cmp_str> Members;
    Members members;
    ZipFile() : p(0) { }
    ZipFile(struct zip * p) : p(p) { }
    ~ZipFile() { if (p) zip_close(p); }
    operator zip *() { return p; }
};


struct ZipMember
{
    struct zip_file * p;
    ZipMember(struct zip_file * p) : p(p) { }
    ~ZipMember() { if (p) zip_fclose(p); }
    operator zip_file *() { return p; }
};


typedef std::map<Path, ZipFile> ZipFiles;
std::map<Path, ZipFile> zipFiles;


/* Return the uncompressed contents of ‘memberName’ from the given zip
   file object. */
static string readFromZip(struct ZipFile & zipFile,
    const string & archivePath, Path memberName, Path & path)
{
    ZipFile::Members::iterator i = zipFile.members.find(memberName.c_str());
    if (i == zipFile.members.end()) {
        Path tmp = memberName + "/default.nix";
        i = zipFile.members.find(tmp.c_str());
        if (i == zipFile.members.end())
            throw Error(format("couldn't find archive member `%1%' in `%2%'")
                % memberName % archivePath);
        memberName = tmp;
        path += "/default.nix";
    }
    
    ZipMember member(zip_fopen_index(zipFile, i->second.index, 0));
    if (!member)
        throw Error(format("couldn't open archive member `%1%' in `%2%': %3%")
            % memberName % archivePath % zip_strerror(zipFile));

    unsigned char * buf = new unsigned char[i->second.size];
    AutoDeleteArray<unsigned char> d(buf);
    if (zip_fread(member, buf, i->second.size) != i->second.size)
        throw Error(format("couldn't read archive member `%1%' in `%2%'")
            % memberName % archivePath);

    return string((char *) buf, i->second.size);
}


string readSourceFile(Path & path)
{
    /* Do a fast check to see if ‘path’ is inside a zip file we
       previously opened. */
    foreach (ZipFiles::iterator, i, zipFiles)
        if (i->second && path.compare(0, i->first.size(), i->first) == 0)
            return readFromZip(i->second, i->first, string(path, i->first.size() + 1), path);

    //printMsg(lvlError, format("NOT FOUND IN OPEN ZIP: %1%") % path);
    
    struct stat st;
    
    if (stat(path.c_str(), &st)) {
        if (errno != ENOTDIR)
            throw SysError(format("getting status of `%1%'") % path);

        /* One of the parents of ‘path’ is (probably) a regular
           file, so it could be an archive.  Look for the first
           parent of ‘path’ that exists and is a regular file. */
        Path archivePath = dirOf(path), memberName = baseNameOf(path);
        do {
            if (stat(archivePath.c_str(), &st) == 0) break;
            if (errno != ENOTDIR)
                throw SysError(format("getting status of `%1%'") % archivePath);
            memberName = baseNameOf(archivePath) + "/" + memberName;
            archivePath = dirOf(archivePath);
            if (archivePath == "/") throw SysError("path `%1%' does not exist");
        } while (true);

        int error;
        struct zip * p = zip_open(archivePath.c_str(), 0, &error);
        if (!p) {
            char errorMsg[1024];
            zip_error_to_str(errorMsg, sizeof errorMsg, error, errno);
            throw Error(format("couldn't open `%1%': %2%") % archivePath % errorMsg);
        }

        ZipFile & zipFile(zipFiles[archivePath]);
        assert(!zipFile);
        zipFile.p = p;

        /* Read the index of the zip file and put it in a map.  This
           is unfortunately necessary because libzip's lookup
           functions are O(n) time. */
        struct zip_stat sb;
        zip_uint64_t nrEntries = zip_get_num_entries(zipFile, 0);
        for (zip_uint64_t n = 0; n < nrEntries; ++n) {
            if (zip_stat_index(zipFile, n, 0, &sb))
                throw Error(format("couldn't stat archive member #%1% in `%2%': %3%")
                    % n % archivePath % zip_strerror(zipFile));
            zipFile.members[sb.name] = sb;
        }
        
        return readFromZip(zipFile, archivePath, memberName, path);
    }

    else {
        /* If `path' refers to a directory, append `/default.nix'. */
        if (S_ISDIR(st.st_mode)) path += "/default.nix";

        if (hasSuffix(path, ".zip")) {
            path += "/default.nix";
            return readSourceFile(path);
        }

        string source = readFile(path);

        /* Handle the case where we're importing the top-level
           (i.e. /default.nix) from a zip file that doesn't have the
           .zip extension.  This is a bit inefficient because we just
           did a complete readFile(). */
        if (string(source, 0, 2) == "PK") {
            path += "/default.nix";
            return readSourceFile(path);
        }
        
        return source;
    }
}


Path copySourceToStore(bool computeOnly, const Path & srcPath,
    PathFilter & filter)
{
    PathDumper dumper(srcPath, true, filter);
    return store->maybeAddToStore(computeOnly, dumper, baseNameOf(srcPath));
}


}
