package org.apache.flink.optimizer.traversals;

import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.util.Visitor;

public class PlanNodeDigestVisitor implements Visitor<PlanNode> {

	@Override
	public boolean preVisit(PlanNode visitable) {
		return false;
	}

	@Override
	public void postVisit(PlanNode visitable) {
		String name = visitable.getNodeName();
//		for (Channel channel : visitable.getInputs()) {
//			channel.getSource()
//		}
	}
}
