#include "path-info.hh"
#include "worker-protocol.hh"

namespace nix {

ValidPathInfo ValidPathInfo::read(Source & source, const Store & store, unsigned int format)
{
    return read(source, store, format, store.parseStorePath(readString(source)));
}

ValidPathInfo ValidPathInfo::read(Source & source, const Store & store, unsigned int format, StorePath && path)
{
    auto deriver = readString(source);
    auto narHash = Hash::parseAny(readString(source), htSHA256);
    ValidPathInfo info(path, narHash);
    if (deriver != "") info.deriver = store.parseStorePath(deriver);
    info.references = worker_proto::read(store, source, Phantom<StorePathSet> {});
    source >> info.registrationTime >> info.narSize;
    if (format >= 16) {
        source >> info.ultimate;
        info.sigs = readStrings<StringSet>(source);
        info.ca = parseContentAddressOpt(readString(source));
    }
    if (format >= 33) {
        if (readInt(source))
            info.owners = worker_proto::read(store, source, Phantom<std::set<StoreUser>> {});
    }
    return info;
}

void ValidPathInfo::write(
    Sink & sink,
    const Store & store,
    unsigned int format,
    bool includePath) const
{
    if (includePath)
        sink << store.printStorePath(path);
    sink << (deriver ? store.printStorePath(*deriver) : "")
         << narHash.to_string(Base16, false);
    worker_proto::write(store, sink, references);
    sink << registrationTime << narSize;
    if (format >= 16) {
        sink << ultimate
             << sigs
             << renderContentAddress(ca);
    }
    if (format >= 33) {
        sink << (bool) owners;
        if (owners)
            worker_proto::write(store, sink, *owners);
    }
}

}
