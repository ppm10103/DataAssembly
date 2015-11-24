package cn.ce.dvs.manager;

import java.util.Observable;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class SignalObserverHandler extends Observable implements SignalHandler {  
	  
    @Override  
    public void handle(Signal signal) {  
        setChanged();  
        notifyObservers(signal);  
    }  

    public void handleSignal(String signalName) throws IllegalArgumentException {  

        try {  

            Signal.handle(new Signal(signalName), this);  

        } catch (IllegalArgumentException x) {  
            throw x;  

        } catch (Throwable x) {  

            throw new IllegalArgumentException("Signal unsupported: "+signalName, x);  
        }  
    }  
}