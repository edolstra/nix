#include "serialise.hh"

namespace nix::daemon {

void processConnection(FdSource & from, FdSink & to, bool trusted);

}
