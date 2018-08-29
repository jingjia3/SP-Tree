package gx.pl.sparql;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;

import java.util.ArrayList;
import java.util.List;

public class MyOpVisitorBase extends OpVisitorBase {
    
    private List<Triple> triples = new ArrayList<Triple>();
    
    public void myOpVisitorWalker(Op op)
    {
        OpWalker.walk(op, this);
    }

    @Override
    public void visit(final OpBGP opBGP) {
        triples = opBGP.getPattern().getList();
    }
    
    public List<Triple> getBGP(){
	return triples;
    }
}

