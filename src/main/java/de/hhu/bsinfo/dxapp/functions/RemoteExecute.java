package de.hhu.bsinfo.dxapp.functions;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.function.DistributableFunction;
import de.hhu.bsinfo.dxram.function.util.ParameterList;

public class RemoteExecute implements DistributableFunction<ParameterList, ParameterList> {
    @Override
    public ParameterList execute(DXRAMServiceAccessor dxramServiceAccessor, ParameterList parameterList) {
        return null;
    }
}
