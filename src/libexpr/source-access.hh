#ifndef __SOURCE_ACCESS_H
#define __SOURCE_ACCESS_H

#include "archive.hh"

namespace nix {

string readSourceFile(Path & path);

Path copySourceToStore(bool computeOnly,
    const Path & srcPath, PathFilter & filter = defaultPathFilter);

}

#endif /* !__SOURCE_ACCESS_H */
